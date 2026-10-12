import argparse
import asyncio
import csv
import importlib.metadata
import json
import platform
import subprocess
import time
from pathlib import Path
from uuid import uuid4
import psutil
import yaml
from .model import SharedModel, ModelFailure
from .recording import Recorder, save_json, utc_now, validate_trace
from .runtime import PROMPT_VERSION, Runtime, workflow
from .evaluation import check_outcome
from .schema import Config
from .aggregation import ParallelRuntime, parallel_workflow, JoinTimeout
from .shared_state import SharedStateRuntime, shared_record_workflow, ORDER_PROMPT_VERSION
from .scheduling import ScheduleTimeout
from .correction import CorrectionRuntime, correction_workflow, PLAN_PROMPT_VERSION

ROOT = Path(__file__).resolve().parents[1]


def command_output(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=5)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


async def monitor(directory, stop, interval):
    fields = ["monotonic_ns", "wall_time_utc", "cpu_percent", "system_ram_used_bytes",
              "system_ram_available_bytes", "process_rss_bytes", "gpu_metrics_json"]
    with (directory / "resource_metrics.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        while True:
            gpu = await asyncio.to_thread(command_output, ["nvidia-smi", "--query-gpu=name,memory.total,memory.used,utilization.gpu", "--format=csv,noheader,nounits"])
            memory = psutil.virtual_memory()
            writer.writerow(dict(zip(fields, [time.perf_counter_ns(), utc_now(), psutil.cpu_percent(),
                                               memory.used, memory.available, psutil.Process().memory_info().rss,
                                               json.dumps({"raw_csv": gpu, "available": gpu is not None})])))
            stream.flush()
            if stop.is_set():
                break
            try:
                await asyncio.wait_for(stop.wait(), interval)
            except TimeoutError:
                pass


async def run(config, output_root):
    run_id = uuid4().hex
    directory = Path(output_root) / f"run_{run_id}"
    directory.mkdir(parents=True)
    (directory / "private").mkdir()
    recorder = Recorder(directory, run_id)
    (directory / "config.resolved.yaml").write_text(yaml.safe_dump(config.model_dump()), encoding="utf-8")
    task = json.loads((ROOT / "tasks" / f"{config.experiment.task_id}.json").read_text(encoding="utf-8"))
    save_json(directory / "task_contract.json", {key: value for key, value in task.items() if not key.startswith("fixture_")})
    shared = config.workflow.template == "shared_record"
    correction = config.workflow.template == "plan_correction"
    stage = {"stage2_facts": 2, "parallel_aggregation": 3, "shared_record": 4, "plan_correction": 5}[config.workflow.template]
    if config.workflow.max_additional_retries:
        stage = 5
    manifest = {"run_id": run_id, "stage": stage, "execution_mode": config.experiment.mode,
                "status": "started", "start_time_utc": utc_now(), "end_time_utc": None,
                "code_commit": command_output(["git", "rev-parse", "HEAD"]),
                "working_tree_status": command_output(["git", "status", "--porcelain"]),
                "dependency_versions": {p: importlib.metadata.version(p) for p in ["httpx", "pydantic", "PyYAML", "psutil"]},
                "machine": {"system": platform.platform(), "python": platform.python_version(),
                            "cpu_count": psutil.cpu_count(), "ram_total_bytes": psutil.virtual_memory().total},
                "prompt_versions": ({"interpret_order": ORDER_PROMPT_VERSION} if shared else
                                    {"select_plan": PLAN_PROMPT_VERSION} if correction else {"extract_facts": PROMPT_VERSION}), "ollama_version": None, "model_digest": None,
                "concurrency_description": "Agents execute asynchronously, with model inference restricted to one shared serving slot.",
                "model_generation_measured": False}
    save_json(directory / "manifest.json", manifest)
    stop = asyncio.Event()
    monitoring = asyncio.create_task(monitor(directory, stop, config.recording.resource_sample_interval_seconds))
    model = None
    runtime = None
    output = None
    infrastructure, model_outcomes = [], []
    operation, attempt = uuid4().hex, uuid4().hex
    begin = recorder.emit("controller", "run", "run", "started", operation, attempt)
    status = "completed"
    try:
        async with asyncio.timeout(config.runtime.run_timeout_seconds):
            model = SharedModel(config.model, config.experiment.mode, config.experiment.replay_directory)
            runtime = (SharedStateRuntime(config, recorder, model) if shared else
                       CorrectionRuntime(config, recorder, model) if correction else
                       ParallelRuntime(config, recorder, model) if stage == 3 else Runtime(config, recorder, model))
            if shared:
                save_json(directory / "private" / "schedule_plan.json", {
                    "mode": config.schedule.mode, "profile": config.schedule.profile,
                    "ordered_checkpoints": runtime.scheduler.plan})
            await model.initialize()
            manifest.update(model_digest=model.model_digest, ollama_version=model.ollama_version)
            output, agents = await (shared_record_workflow(runtime, task) if shared else
                                   correction_workflow(runtime, task) if correction else
                                   parallel_workflow(runtime, task) if stage == 3 else workflow(runtime, task))
            manifest["agents"] = [{"agent_id": a.agent_id, "role": a.role, "status": a.status,
                                   "actions": a.actions, "local_sequence": a.local_sequence} for a in agents]
    except ModelFailure as exc:
        model_outcomes.append(str(exc))
        status = "failed"
    except JoinTimeout:
        infrastructure.append("join_timeout")
        status = "timed_out"
    except ScheduleTimeout:
        infrastructure.append("schedule_gate_timeout")
        status = "timed_out"
    except TimeoutError:
        infrastructure.append("run_timeout")
        status = "timed_out"
    except Exception as exc:
        infrastructure.append(f"{type(exc).__name__}: {exc}")
        status = "failed"
    finally:
        if model:
            await model.close()
        stop.set()
        try:
            await monitoring
        except Exception as exc:
            infrastructure.append("resource_monitor: " + str(exc))
            status = "failed"
    from .schema import DependencyRef
    if isinstance(runtime, SharedStateRuntime):
        save_json(directory / "state_history.json", {"records": runtime.state.history(task["key"])})
        save_json(directory / "observed_schedule.json", {"checkpoints": runtime.scheduler.observed})
        schedule_assessment = runtime.scheduler.assessment()
        save_json(directory / "private" / "schedule_assessment.json", schedule_assessment)
        if status == "completed" and not schedule_assessment["constraints_satisfied"]:
            infrastructure.append("schedule_not_reproduced")
            status = "failed"
    recorder.emit("controller", "run", "run", status, operation, attempt,
                  outputs=[recorder.payload(output)],
                  dependencies=[DependencyRef(event_id=begin.event_id, relationship="operation_start"),
                      *[DependencyRef(event_id=e.event_id, relationship="accepted_attempt") for e in recorder.events
                        if e.event_type == "operation_result" and e.status == "completed"]],
                  infrastructure_failures=infrastructure, model_outcomes=model_outcomes)
    save_json(directory / "final_output.json", output)
    validation = validate_trace(directory)
    save_json(directory / "trace_validation.json", validation)
    assessment = check_outcome(task, output)
    assessment.update(trace_validity=validation["valid"], infrastructure_failures=infrastructure,
                      model_outcomes=model_outcomes, execution_mode=config.experiment.mode)
    save_json(directory / "private" / "outcome_assessment.json", assessment)
    injection = runtime.injection if isinstance(runtime, (ParallelRuntime, SharedStateRuntime)) else {"enabled": False, "status": "not_requested"}
    if injection.get("enabled"):
        injection["status"] = ("invalid_infrastructure" if infrastructure else
                               "activated_and_task_failed" if injection["activated"] and assessment["task_correctness"] == "failure" else
                               "activated_recovered" if injection["activated"] and assessment.get("recovered") else
                               "activated_harmless" if injection["activated"] and assessment["task_correctness"] == "success" else
                               "activated_outcome_unknown" if injection["activated"] else "not_activated")
    save_json(directory / "private" / "injection_manifest.json", {**injection, "stage": stage})
    save_json(directory / "private" / "reference_labels.json", {
        "task_outcome": assessment["task_correctness"], "injection_status": injection["status"],
        "first_observed_contract_violation": None, "candidate_origin_events": [],
        "missed_recovery_events": [], "supported_cause_events": [], "intervention_result": "unknown",
        "review_status": "unreviewed", "label_notes": "Injection activation is not a confirmed root cause. No failure attribution is performed."})
    # Export only explicit observed references; no harmful-cause inference or workflow-required edges.
    save_json(directory / "observed_dependencies.json", {
        "edges": [{"source": dep.event_id, "target": e.event_id, "relationship": dep.relationship}
                  for e in recorder.events for dep in e.dependency_refs]})
    manifest.update(status=status, end_time_utc=utc_now(), trace_validity=validation["valid"],
                    task_correctness=assessment["task_correctness"])
    save_json(directory / "manifest.json", manifest)
    passed = status == "completed" and validation["valid"] and assessment["task_correctness"] == "success"
    return directory, passed


def main():
    parser = argparse.ArgumentParser(description="Stage 5 correction and bounded retries; defaults to model-free scripted fixtures")
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "stage5.yaml")
    parser.add_argument("--output-root", type=Path, default=ROOT / "runs")
    parser.add_argument("--mode", choices=["scripted_fixture", "live", "recorded_response"])
    parser.add_argument("--replay-directory")
    args = parser.parse_args()
    data = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    if args.mode:
        data["experiment"]["mode"] = args.mode
    if args.replay_directory:
        data["experiment"]["replay_directory"] = args.replay_directory
    config = Config.model_validate(data)
    directory, passed = asyncio.run(run(config, args.output_root))
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    print(json.dumps({"run_directory": str(directory.resolve()), "passed": passed, "execution_mode": config.experiment.mode,
                      "status": manifest["status"], "task_correctness": manifest["task_correctness"],
                      "trace_validity": manifest["trace_validity"]}))
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
