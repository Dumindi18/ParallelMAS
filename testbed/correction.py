"""Template B shares the existing instrumented transport, model client and runtime."""
import asyncio
import json
from .aggregation import ParallelRuntime, Receipt
from .faults import CorrectionFaultController
from .recording import canonical
from .runtime import Agent
from .schema import DeliveryPlan, DependencyRef
from .retries import bounded_tool
from .model import ModelFailure

PLAN_PROMPT_VERSION = "1.0"


class CorrectionRuntime(ParallelRuntime):
    def __init__(self, config, recorder, model):
        super().__init__(config, recorder, model)
        self.fault_controller = CorrectionFaultController(config)
        self.injection = self.fault_controller.record
        self.execution_finished = asyncio.Event()
        self.checker_sent = asyncio.Event()
        self.checker_send_event = None

    async def _deliver(self, message, send_event_id):
        transport = Agent(f"transport_{message.sender_id}_{message.receiver_id}", "message delivery")
        async def deliver(evidence):
            if message.message_type in {"approval", "correction"}:
                self.fault_controller.correction_delivery(message, evidence["_terminal_event_id"])
                if self.config.workflow.correction_policy == "after_execution":
                    await self.execution_finished.wait()
                    evidence["_dependencies"] = [DependencyRef(event_id=self.execution_event_id, relationship="schedule_order")]
                if self.config.workflow.correction_delay_seconds:
                    await asyncio.sleep(self.config.workflow.correction_delay_seconds)
            evidence["_input_refs"] = [message.payload_reference]
            return message
        _, event, _ = await self.action(transport, "deliver_plan", "message_delivered", message.model_dump(), deliver,
            [DependencyRef(event_id=send_event_id, relationship="message_send_delivery")],
            {**message.model_dump(), "send_event_id": send_event_id})
        self.agents[message.receiver_id].inbox.put_nowait(Receipt(message, send_event_id, event.event_id))


async def correction_workflow(runtime, task):
    planner = Agent("agent_planner", "You select a delivery option under the supplied constraints.")
    checker = Agent("agent_checker", "You check delivery constraints and explicitly approve a plan version.")
    executor = Agent("agent_executor", "You execute the plan version actually consumed.")
    runtime.register(planner, checker, executor)
    checked = None

    async def plan():
        public = {key: task[key] for key in ["task_id", "instruction", "options", "requirements"]}
        messages = [{"role": "system", "content": planner.role + " Return JSON with option_id and version: 1. Copy option_id exactly from an offered option. Follow the task instruction, budget and prohibited-route constraint."},
                    {"role": "user", "content": canonical(public).decode("utf-8")}]
        initial, event, _ = await runtime.call_structured_model(planner, "select_plan", messages,
            task["fixture_plan"], DeliveryPlan)
        if initial.option_id not in {option["option_id"] for option in task["options"]}:
            raise ModelFailure("unknown_delivery_option")
        for recipient in [checker, executor]:
            await runtime.send_message(planner, recipient.agent_id, initial.model_dump(), event.event_id, "initial_plan")
        planner.status = "completed"

    async def check():
        nonlocal checked
        receipt = await runtime.receive_message(checker)
        initial, consumed, _ = await runtime.consume_message(checker, receipt, "use_initial_plan", "check_plan")
        def approve(arguments):
            selected = next(option for option in task["options"] if option["option_id"] == arguments["plan"]["option_id"])
            compliant = lambda option: option["cost_cents"] <= task["requirements"]["budget_cents"] and option["route"] not in task["requirements"]["prohibited_routes"]
            correction = not compliant(selected)
            if correction:
                selected = next(option for option in task["options"] if compliant(option))
            return {"option_id": selected["option_id"], "version": 2 if correction else 1,
                    "approved": True, "correction_required": correction,
                    "previous_version": arguments["plan"]["version"]}
        checked, event, _ = await runtime.call_tool(checker, "check_plan", "delivery_constraints",
            {"plan": initial, "options": task["options"], "requirements": task["requirements"]}, approve,
            [DependencyRef(event_id=consumed.event_id, relationship="produced_output")])
        await runtime.send_message(checker, executor.agent_id, checked, event.event_id,
            "correction" if checked["correction_required"] else "approval")
        runtime.checker_send_event = runtime.recorder.previous[checker.agent_id]
        runtime.checker_sent.set()
        checker.status = "completed"

    async def execute():
        initial_receipt = await runtime.receive_message(executor)
        initial, consumed, _ = await runtime.consume_message(executor, initial_receipt, "use_initial_plan", "execute_delivery")
        dependencies = [DependencyRef(event_id=consumed.event_id, relationship="produced_output")]
        selected, approval_used = initial, False
        policy = runtime.config.workflow.correction_policy
        if policy != "after_execution":
            receipt = await runtime.receive_message(executor)
            if policy == "wait":
                selected, correction_event, _ = await runtime.consume_message(executor, receipt, "use_checked_plan", "execute_delivery")
                dependencies.append(DependencyRef(event_id=correction_event.event_id, relationship="produced_output"))
                approval_used = True
        else:
            # A named gate ensures the correction was sent before the dependent action.
            gate_actor = Agent("correction_gate", "wait for checker send")
            async def gate(evidence):
                await runtime.checker_sent.wait()
                evidence["_dependencies"] = [DependencyRef(event_id=runtime.checker_send_event, relationship="schedule_order")]
                return {"checkpoint": "checker_sent_before_execution"}
            _, gate_event, _ = await runtime.action(gate_actor, "checker_sent", "correction_gate", {}, gate)
            dependencies.append(DependencyRef(event_id=gate_event.event_id, relationship="schedule_order"))
        result = await bounded_tool(runtime, executor, {"plan": selected, "initial_plan": initial,
                                    "approval_consumed": approval_used}, dependencies)
        runtime.execution_event_id = runtime.recorder.previous[executor.agent_id]
        runtime.execution_finished.set()
        executor.status = "completed"
        return {**result, "approval_consumed": approval_used}

    jobs = [asyncio.create_task(plan()), asyncio.create_task(check()), asyncio.create_task(execute())]
    try:
        await asyncio.gather(*jobs)
        await runtime.drain_deliveries()
        result = jobs[-1].result()
        correction_recovered = result["executed"] and result["approval_consumed"] and checked["correction_required"]
        return {**result, "approved_plan": checked, "tool_recovered": result["recovered"],
                "correction_recovered": correction_recovered,
                "recovered": result["recovered"] or correction_recovered}, [planner, checker, executor]
    except BaseException:
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        raise
    finally:
        await runtime.close_deliveries()


def validate_correction_evidence(directory, events):
    errors = []
    def value(ref):
        return json.loads((directory / "payloads" / f"{ref.payload_id}.json").read_text(encoding="utf-8"))
    consumed = {e.event_id: e for e in events if e.event_type == "message_consumed" and e.status == "completed"
                and e.details["message_type"] != "worker_result"}
    uses = set()
    for event in events:
        if event.status == "started" or event.step_id not in {"check_plan", "execute_delivery"} or event.event_type != "tool_call":
            continue
        try:
            args = value(event.input_refs[0])
            inputs = [consumed[d.event_id] for d in event.dependency_refs if d.event_id in consumed]
            if not inputs or not any(value(source.output_refs[0]) == args["plan"] for source in inputs):
                errors.append("dependent action did not use a consumed plan")
            if any(value(source.output_refs[0]) not in [args["plan"], args.get("initial_plan")] for source in inputs):
                errors.append("consumed plan omitted from dependent action input")
            uses.update(source.event_id for source in inputs)
            approvals = [source for source in inputs if source.details["message_type"] in {"approval", "correction"}]
            if event.step_id == "execute_delivery" and args["approval_consumed"] != bool(approvals):
                errors.append("approval consumption contradicts execution input")
        except (OSError, ValueError, KeyError, IndexError):
            errors.append("invalid correction action evidence")
    # Failed model/checker branches may leave an assembled consumption with no terminal tool.
    run_completed = any(e.event_type == "run" and e.status == "completed" for e in events)
    if run_completed and set(consumed) != uses:
        errors.append("plan consumption has no matching dependent action")
    if run_completed and any(e.step_id == "execute_delivery" for e in events):
        try:
            final = json.loads((directory / "final_output.json").read_text(encoding="utf-8"))
            actions = [e for e in events if e.step_id == "execute_delivery" and e.event_type == "tool_call" and e.status != "started"]
            last = actions[-1]
            args = value(last.input_refs[0])
            checker = next(e for e in events if e.step_id == "check_plan" and e.status == "completed" and e.event_type == "tool_call")
            if (final["plan"] != args["plan"] or final["approval_consumed"] != args["approval_consumed"]
                    or final["approved_plan"] != value(checker.output_refs[0])
                    or final["executed"] != (last.status == "completed")
                    or final["accepted_attempt_event_id"] != (last.event_id if final["executed"] else None)
                    or final["tool_recovered"] != (final["executed"] and last.details["attempt_number"] > 1)
                    or final["correction_recovered"] != (final["executed"] and final["approval_consumed"] and final["approved_plan"]["correction_required"])
                    or final["recovered"] != (final["tool_recovered"] or final["correction_recovered"])):
                errors.append("final delivery differs from accepted attempt evidence")
        except (OSError, ValueError, KeyError, IndexError, StopIteration):
            errors.append("missing/invalid final correction output")
    return errors
