import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import httpx
import yaml
from pydantic import ValidationError
from testbed.__main__ import ROOT, run
from testbed.model import SharedModel
from testbed.recording import validate_trace
from testbed.schema import Config


def config(name="stage3.yaml", **workflow_changes):
    data = yaml.safe_load((ROOT / "configs" / name).read_text())
    data.setdefault("workflow", {}).update(workflow_changes)
    return Config.model_validate(data)


def read(path, filename):
    return json.loads((path / filename).read_text(encoding="utf-8"))


def events(path):
    return [json.loads(line) for line in (path / "events.jsonl").read_text().splitlines()]


class Stage3Tests(unittest.IsolatedAsyncioTestCase):
    async def test_reference_three_workers_and_harmless_delay(self):
        for name, quantity, cost, workers in [("stage3.yaml", 10, 1475, 2),
                                              ("stage3_three_workers.yaml", 12, 2900, 3),
                                              ("stage3_delayed.yaml", 10, 1475, 2)]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                path, passed = await run(config(name), tmp)
                self.assertTrue(passed, read(path, "trace_validation.json"))
                result = read(path, "final_output.json")
                self.assertEqual(result["total_quantity"], quantity)
                self.assertEqual(result["total_cost_cents"], cost)
                completed = [e for e in events(path) if e["status"] == "completed"]
                for kind in ["message_sent", "message_delivered", "message_received", "message_consumed"]:
                    self.assertEqual(sum(e["event_type"] == kind for e in completed), workers)
                join = next(e for e in completed if e["event_type"] == "join")
                self.assertEqual(join["details"]["release_reason"], "all_required_inputs")
                self.assertEqual(join["details"]["missing_branches"], [])
                self.assertEqual(len(join["details"]["accepted_result_ids"]), workers)
                model_starts = [e["monotonic_ns"] for e in events(path) if e["event_type"] == "model_request" and e["status"] == "started"]
                model_ends = [e["monotonic_ns"] for e in completed if e["event_type"] == "model_request"]
                self.assertLess(max(model_starts), min(model_ends))
                self.assertNotIn("fixture_facts", read(path, "task_contract.json"))

    async def test_premature_join_activates_and_late_result_is_never_consumed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, passed = await run(config("stage3_premature.yaml"), tmp)
            self.assertFalse(passed)
            self.assertTrue(read(path, "trace_validation.json")["valid"])
            self.assertEqual(read(path, "manifest.json")["status"], "completed")
            assessment = read(path, "private/outcome_assessment.json")
            self.assertIn("missing_required_worker_results", assessment["execution_contract_violations"])
            injection = read(path, "private/injection_manifest.json")
            self.assertTrue(injection["activated"])
            self.assertEqual(injection["activation_count"], 1)
            self.assertEqual(injection["status"], "activated_and_task_failed")
            completed = [e for e in events(path) if e["status"] == "completed"]
            late = next(e for e in completed if e["event_type"] == "message_delivered" and e["details"]["sender_id"] == "agent_worker_2")
            aggregate = next(e for e in completed if e["step_id"] == "aggregate_orders")
            self.assertGreater(late["monotonic_ns"], aggregate["monotonic_ns"])
            late_id = late["details"]["message_id"]
            self.assertFalse(any(e["event_type"] == "message_consumed" and e["details"]["message_id"] == late_id for e in completed))
            join = next(e for e in completed if e["event_type"] == "join")
            self.assertIn("agent_worker_2", join["details"]["missing_branches"])
            late_source = late["details"]["source_event_id"]
            self.assertFalse(any(dep["event_id"] == late_source and dep["relationship"] == "accepted_branch_result" for dep in join["dependency_refs"]))

    async def test_join_timeout_preserves_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, passed = await run(config(join_timeout_seconds=0.01, message_delays={"agent_worker_2": 0.2}), tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"])
            self.assertEqual(read(path, "manifest.json")["status"], "timed_out")
            join = next(e for e in events(path) if e["event_type"] == "join" and e["status"] == "timed_out")
            self.assertTrue(join["details"]["timeout_status"])
            self.assertIn("agent_worker_2", join["details"]["missing_branches"])

    async def test_semantic_validator_rejects_lifecycle_and_join_tampering(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = await run(config(), tmp)
            original = events(path)
            for violation in ["delivery", "join"]:
                altered = json.loads(json.dumps(original))
                if violation == "delivery":
                    e = next(e for e in altered if e["event_type"] == "message_consumed" and e["status"] == "completed")
                    e["details"]["delivery_event_id"] = "wrong_delivery"
                else:
                    e = next(e for e in altered if e["event_type"] == "join" and e["status"] == "completed")
                    e["details"]["accepted_result_ids"] = []
                (path / "events.jsonl").write_text("\n".join(json.dumps(e) for e in altered) + "\n")
                self.assertFalse(validate_trace(path)["valid"])

    async def test_live_mode_with_mock_http_and_exact_replay(self):
        async def mock(request):
            if request.url.path == "/api/version":
                return httpx.Response(200, json={"version": "mock"})
            if request.url.path == "/api/tags":
                return httpx.Response(200, json={"models": [{"name": "qwen3:8b", "digest": "mock-digest"}]})
            body = json.loads(request.content)
            data = json.loads(body["messages"][1]["content"])
            self.assertEqual(set(data), {"document_id", "document"})
            self.assertFalse(body["think"])
            quantity, price = (7, 125) if data["document_id"] == "department_A" else (3, 200)
            facts = {"document_id": data["document_id"], "quantity": quantity, "unit_cost_cents": price}
            return httpx.Response(200, json={"done": True, "done_reason": "stop", "message": {"content": json.dumps(facts)}, "prompt_eval_count": 100, "eval_count": 20})
        def factory(model_config, mode, replay_directory):
            return SharedModel(model_config, mode, replay_directory, transport=httpx.MockTransport(mock))
        with tempfile.TemporaryDirectory() as tmp:
            live = config()
            live.experiment.mode = "live"
            with patch("testbed.__main__.SharedModel", factory):
                source, passed = await run(live, tmp)
            self.assertTrue(passed)
            replay = config()
            replay.experiment.mode = "recorded_response"
            replay.experiment.replay_directory = str(source)
            _, passed = await run(replay, tmp)
            self.assertTrue(passed)
            changed = config()
            changed.experiment.mode = "recorded_response"
            changed.experiment.replay_directory = str(source)
            changed.experiment.seed = 43
            path, passed = await run(changed, tmp)
            self.assertFalse(passed)
            self.assertIn("recorded_response_mismatch", read(path, "private/outcome_assessment.json")["model_outcomes"])

    def test_premature_requires_explicit_fault_and_valid_configuration(self):
        with self.assertRaises(ValidationError):
            config(join_release_condition="premature")
        with self.assertRaises(ValidationError):
            config(message_delays={"nonexistent_worker": 1})
        with self.assertRaises(ValidationError):
            config(hold_branch_until_aggregation="agent_worker_2")
