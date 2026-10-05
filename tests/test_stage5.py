import json
import tempfile
import unittest
from unittest.mock import patch
import httpx
import yaml
from pydantic import ValidationError
from testbed.__main__ import ROOT, run
from testbed.schema import Config
from testbed.model import SharedModel, ModelFailure
from testbed.recording import validate_trace
from testbed.faults import CorrectionFaultController


def config(name="stage5"):
    return Config.model_validate(yaml.safe_load((ROOT / "configs" / f"{name}.yaml").read_text()))


def read(path, name):
    return json.loads((path / name).read_text(encoding="utf-8"))


def events(path):
    return [json.loads(line) for line in (path / "events.jsonl").read_text().splitlines()]


class Stage5Tests(unittest.IsolatedAsyncioTestCase):
    async def test_correction_used_before_action_and_harmless_delay(self):
        for name in ["stage5", "stage5_harmless_delay"]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                path, passed = await run(config(name), tmp)
                self.assertTrue(passed, read(path, "trace_validation.json"))
                final = read(path, "final_output.json")
                self.assertEqual(final["plan"]["version"], 2)
                self.assertEqual(final["plan"]["option_id"], "standard")
                self.assertTrue(final["correction_recovered"])
                self.assertFalse(final["tool_recovered"])
                trace = events(path)
                consumed = next(e for e in trace if e["event_type"] == "message_consumed" and e["status"] == "completed" and e["details"]["message_type"] == "correction")
                action = next(e for e in trace if e["step_id"] == "execute_delivery" and e["status"] == "completed")
                self.assertLess(consumed["monotonic_ns"], action["monotonic_ns"])
                self.assertIn(consumed["event_id"], [d["event_id"] for d in action["dependency_refs"]])
                self.assertNotIn("fixture_plan", read(path, "task_contract.json"))
                self.assertTrue(all(a["actions"] <= 8 for a in read(path, "manifest.json")["agents"]))

    async def test_late_and_unconsumed_corrections_are_distinct_valid_failures(self):
        for name in ["stage5_delayed", "stage5_unconsumed"]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                path, passed = await run(config(name), tmp)
                self.assertFalse(passed)
                self.assertTrue(validate_trace(path)["valid"])
                trace = events(path)
                delivery = next(e for e in trace if e["event_type"] == "message_delivered" and e["status"] == "completed" and e["details"]["message_type"] == "correction")
                action = next(e for e in trace if e["step_id"] == "execute_delivery" and e["status"] == "completed")
                self.assertEqual(delivery["monotonic_ns"] > action["monotonic_ns"], name == "stage5_delayed")
                self.assertFalse(any(e["event_type"] == "message_consumed" and e["details"]["message_type"] == "correction" for e in trace))
                injection = read(path, "private/injection_manifest.json")
                self.assertEqual(injection["status"], "activated_and_task_failed")
                self.assertEqual(injection["activation_count"], 1)
                self.assertEqual(read(path, "private/outcome_assessment.json")["infrastructure_failures"], [])

    async def test_mock_tool_failure_and_single_retry_recovery(self):
        for name, recovered in [("stage5_tool_failure", False), ("stage5_tool_recovery", True)]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                path, passed = await run(config(name), tmp)
                self.assertEqual(passed, recovered)
                self.assertTrue(validate_trace(path)["valid"])
                attempts = [e for e in events(path) if e["step_id"] == "execute_delivery" and e["status"] != "started"]
                self.assertEqual(len(attempts), 2 if recovered else 1)
                self.assertEqual(attempts[0]["status"], "failed")
                if recovered:
                    self.assertEqual(attempts[0]["operation_id"], attempts[1]["operation_id"])
                    self.assertNotEqual(attempts[0]["attempt_id"], attempts[1]["attempt_id"])
                    self.assertEqual(attempts[1]["details"]["previous_attempt"], attempts[0]["attempt_id"])
                    self.assertEqual(read(path, "final_output.json")["accepted_attempt_event_id"], attempts[1]["event_id"])
                self.assertEqual(read(path, "private/injection_manifest.json")["status"], "activated_recovered" if recovered else "activated_and_task_failed")

    async def test_persistent_tool_failure_stops_at_two_attempts(self):
        def always_fail(controller, number, arguments, event_id):
            controller.activate(event_id, "success", "failure")
            return True
        with tempfile.TemporaryDirectory() as tmp, patch.object(CorrectionFaultController, "fail_tool", always_fail):
            path, passed = await run(config("stage5_tool_recovery"), tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"])
            decisions = [e for e in events(path) if e["event_type"] == "retry_decision" and e["status"] == "completed"]
            self.assertEqual([e["details"]["retry"] for e in decisions], [True, False])

    async def test_inventory_recovery_refreshes_and_rebuilds_proposal(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, passed = await run(config("stage5_conflict_recovery"), tmp)
            self.assertTrue(passed, read(path, "trace_validation.json"))
            final = read(path, "final_output.json")
            self.assertEqual(final["final_record"]["value"]["available_quantity"], 3)
            self.assertEqual([u["attempts"] for u in final["updates"]], [1, 2])
            self.assertEqual(final["updates"][1]["read_version"], 1)
            trace = events(path)
            writes = [e for e in trace if e["event_type"] == "state_write" and e["status"] == "completed" and e["agent_id"] == "agent_updater_B"]
            self.assertEqual([e["details"]["accepted"] for e in writes], [False, True])
            self.assertEqual(writes[0]["operation_id"], writes[1]["operation_id"])
            self.assertEqual(writes[1]["details"]["base_version"], 1)
            self.assertEqual(len([e for e in trace if e["event_type"] == "model_request" and e["status"] == "completed"]), 2)
            self.assertEqual([r["version"] for r in read(path, "state_history.json")["records"]], [0, 1, 2])
            self.assertEqual(read(path, "manifest.json")["agents"][1]["actions"], 7)

    async def test_optional_approval_does_not_turn_allowed_early_action_into_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, passed = await run(config("stage5_optional_approval"), tmp)
            self.assertTrue(passed)
            self.assertFalse(read(path, "final_output.json")["approval_consumed"])
            self.assertFalse(read(path, "private/injection_manifest.json")["activated"])

    async def test_tampered_retry_and_final_selection_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = await run(config("stage5_tool_recovery"), tmp)
            original = events(path)
            changed = json.loads(json.dumps(original))
            second = next(e for e in changed if e["step_id"] == "execute_delivery" and e["status"] == "completed")
            second["details"]["previous_attempt"] = "wrong"
            (path / "events.jsonl").write_text("\n".join(json.dumps(e) for e in changed) + "\n")
            self.assertFalse(validate_trace(path)["valid"])
            (path / "events.jsonl").write_text("\n".join(json.dumps(e) for e in original) + "\n")
            final = read(path, "final_output.json")
            final["plan"]["version"] = 1
            (path / "final_output.json").write_text(json.dumps(final))
            self.assertFalse(validate_trace(path)["valid"])

    async def test_mocked_live_model_and_exact_replay_without_fixture_leakage(self):
        async def mock(request):
            if request.url.path == "/api/version":
                return httpx.Response(200, json={"version": "mock"})
            if request.url.path == "/api/tags":
                return httpx.Response(200, json={"models": [{"name": "qwen3:8b", "digest": "mock-digest"}]})
            body = json.loads(request.content)
            self.assertFalse(body["think"])
            self.assertEqual(set(json.loads(body["messages"][1]["content"])), {"task_id", "instruction", "options", "requirements"})
            return httpx.Response(200, json={"done": True, "message": {"content": '{"option_id":"standard","version":1}'}})
        def factory(model_config, mode, replay_directory):
            return SharedModel(model_config, mode, replay_directory, transport=httpx.MockTransport(mock))
        with tempfile.TemporaryDirectory() as tmp:
            live = config("stage5_tool_recovery")
            live.experiment.mode = "live"
            with patch("testbed.__main__.SharedModel", factory):
                source, passed = await run(live, tmp)
            self.assertTrue(passed)
            replay = config("stage5_tool_recovery")
            replay.experiment.mode = "recorded_response"
            replay.experiment.replay_directory = str(source)
            _, passed = await run(replay, tmp)
            self.assertTrue(passed)
            replay.experiment.seed = 43
            path, passed = await run(replay, tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"])
            self.assertIn("recorded_response_mismatch", read(path, "private/outcome_assessment.json")["model_outcomes"])

    async def test_model_failure_is_not_retried_as_tool_failure(self):
        async def fail(*args, **kwargs):
            raise ModelFailure("malformed_model_output")
        with tempfile.TemporaryDirectory() as tmp, patch.object(SharedModel, "generate", fail):
            path, passed = await run(config("stage5_tool_recovery"), tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"])
            self.assertFalse(any(e["event_type"] == "retry_decision" for e in events(path)))

    def test_bounded_explicit_fault_and_budget_configuration(self):
        for change in ["retry_limit", "multiple_faults", "implicit_fault", "step_budget"]:
            data = config("stage5_tool_recovery").model_dump()
            if change == "retry_limit":
                data["workflow"]["max_additional_retries"] = 2
            elif change == "multiple_faults":
                data["workflow"]["correction_policy"] = "after_execution"
            elif change == "implicit_fault":
                data["fault"]["enabled"] = False
            else:
                data["runtime"]["max_steps_per_agent"] = 5
            with self.subTest(change=change), self.assertRaises(ValidationError):
                Config.model_validate(data)
