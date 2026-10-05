import asyncio
import json
import tempfile
import unittest
from unittest.mock import patch
import httpx
import yaml
from pydantic import ValidationError
from testbed.__main__ import ROOT, run
from testbed.model import SharedModel, ModelFailure
from testbed.recording import validate_trace
from testbed.schema import Config
from testbed.state import VersionedState


def config(name="stage4.yaml"):
    return Config.model_validate(yaml.safe_load((ROOT / "configs" / name).read_text()))


def read(path, file):
    return json.loads((path / file).read_text(encoding="utf-8"))


def events(path):
    return [json.loads(line) for line in (path / "events.jsonl").read_text().splitlines()]


class Stage4Tests(unittest.IsolatedAsyncioTestCase):
    async def test_state_is_atomic_versioned_and_snapshots_are_private_copies(self):
        state = VersionedState()
        await state.initialize("inventory", {"count": 10}, "initial")
        initial = await state.read("inventory")
        initial["record"]["value"]["count"] = -100
        self.assertEqual((await state.read("inventory"))["record"]["value"]["count"], 10)
        first, second = await asyncio.gather(
            state.write("inventory", {"count": 9}, 0, "compare_and_set", "first"),
            state.write("inventory", {"count": 8}, 0, "compare_and_set", "second"))
        self.assertEqual(sum(result["accepted"] for result in [first, second]), 1)
        self.assertEqual(len(state.history("inventory")), 2)
        conflict = next(result for result in [first, second] if not result["accepted"])
        self.assertEqual(conflict["rejection_reason"], "version_conflict")
        overwritten = await state.write("inventory", {"count": 7}, 0, "unconditional", "third")
        self.assertTrue(overwritten["accepted"])
        self.assertEqual(overwritten["previous_current_version"], 1)
        self.assertEqual(overwritten["new_version"], 2)
        unchanged = await state.write("inventory", {"count": 7}, 2, "compare_and_set", "fourth")
        self.assertEqual(unchanged["new_version"], 3)
        self.assertEqual((await state.read("inventory", 0))["record"]["writer_event_id"], "initial")
        with self.assertRaises(ValueError):
            await state.read("inventory", -1)

    async def test_reference_reverse_order_and_second_task(self):
        for name, available in [("stage4.yaml", 3), ("stage4_reverse.yaml", 3), ("stage4_second_task.yaml", 9)]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                path, passed = await run(config(name), tmp)
                self.assertTrue(passed, read(path, "trace_validation.json"))
                self.assertEqual(read(path, "final_output.json")["final_record"]["value"]["available_quantity"], available)
                history = read(path, "state_history.json")["records"]
                self.assertEqual([record["version"] for record in history], [0, 1, 2])
                self.assertEqual(len(history[-1]["value"]["accepted_orders"]), 2)
                self.assertTrue(read(path, "private/schedule_assessment.json")["constraints_satisfied"])
                self.assertNotIn("fixture_decisions", read(path, "task_contract.json"))

    async def test_lost_update_is_reproducible_and_preserves_overwritten_history(self):
        observed_orders = []
        for _ in range(3):
            with tempfile.TemporaryDirectory() as tmp:
                path, passed = await run(config("stage4_lost_update.yaml"), tmp)
                self.assertFalse(passed)
                self.assertTrue(read(path, "trace_validation.json")["valid"])
                final = read(path, "final_output.json")
                self.assertEqual(final["final_record"]["value"]["available_quantity"], 6)
                self.assertEqual([item["read_version"] for item in final["updates"]], [0, 0])
                self.assertTrue(all(item["write_accepted"] for item in final["updates"]))
                history = read(path, "state_history.json")["records"]
                self.assertEqual(history[1]["value"]["accepted_orders"][0]["order_id"], "order_A")
                self.assertEqual(history[2]["value"]["accepted_orders"][0]["order_id"], "order_B")
                injection = read(path, "private/injection_manifest.json")
                self.assertTrue(injection["activated"])
                self.assertEqual(injection["activation_count"], 1)
                self.assertEqual(injection["status"], "activated_and_task_failed")
                self.assertIn("accepted_update_overwritten", read(path, "private/outcome_assessment.json")["execution_contract_violations"])
                checkpoints = [item["checkpoint"] for item in read(path, "observed_schedule.json")["checkpoints"] if "updater" in item["checkpoint"]]
                observed_orders.append(checkpoints)
        self.assertTrue(all(order == observed_orders[0] for order in observed_orders))
        self.assertEqual(observed_orders[0], ["agent_updater_A.read_inventory", "agent_updater_B.read_inventory", "agent_updater_A.write_inventory", "agent_updater_B.write_inventory"])

    async def test_stale_read_points_to_old_writer_and_records_actual_current_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, passed = await run(config("stage4_stale_state.yaml"), tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"])
            read_event = next(event for event in events(path) if event["event_type"] == "state_read" and event["agent_id"] == "agent_updater_B" and event["status"] == "completed")
            self.assertEqual(read_event["details"]["returned_version"], 0)
            self.assertEqual(read_event["details"]["current_version"], 1)
            history = read(path, "state_history.json")["records"]
            self.assertEqual(read_event["details"]["originating_write_event_id"], history[0]["writer_event_id"])
            self.assertTrue(read(path, "private/injection_manifest.json")["activated"])
            self.assertIn("stale_snapshot_used", read(path, "private/outcome_assessment.json")["execution_contract_violations"])

    async def test_compare_and_set_conflict_does_not_create_a_version_or_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, passed = await run(config("stage4_conflict.yaml"), tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"])
            self.assertEqual(len(read(path, "state_history.json")["records"]), 2)
            writes = [event for event in events(path) if event["event_type"] == "state_write" and event["status"] == "completed"]
            self.assertEqual(len(writes), 2)
            self.assertEqual([event["details"]["accepted"] for event in writes], [True, False])
            self.assertIsNone(writes[1]["details"]["new_version"])
            self.assertFalse(read(path, "private/injection_manifest.json")["enabled"])

    async def test_natural_mode_records_order_without_claiming_constraints(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = await run(config("stage4_natural.yaml"), tmp)
            self.assertTrue(validate_trace(path)["valid"])
            self.assertEqual(read(path, "private/schedule_plan.json")["ordered_checkpoints"], [])
            self.assertEqual(len(read(path, "observed_schedule.json")["checkpoints"]), 5)

    async def test_trace_validator_rejects_state_history_and_provenance_tampering(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, _ = await run(config(), tmp)
            original = events(path)
            modified = json.loads(json.dumps(original))
            read_event = next(event for event in modified if event["event_type"] == "state_read" and event["status"] == "completed")
            read_event["details"]["originating_write_event_id"] = "missing_writer"
            (path / "events.jsonl").write_text("\n".join(json.dumps(event) for event in modified) + "\n")
            self.assertFalse(validate_trace(path)["valid"])
            (path / "events.jsonl").write_text("\n".join(json.dumps(event) for event in original) + "\n")
            history = read(path, "state_history.json")
            history["records"][1]["value"]["available_quantity"] = 999
            (path / "state_history.json").write_text(json.dumps(history))
            self.assertFalse(validate_trace(path)["valid"])

    async def test_failure_cancels_waiting_gates_without_event_loss(self):
        async def fail(*args, **kwargs):
            raise ModelFailure("malformed_model_output")
        with tempfile.TemporaryDirectory() as tmp, patch.object(SharedModel, "generate", fail):
            path, passed = await run(config(), tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"], read(path, "trace_validation.json"))
            assessment = read(path, "private/schedule_assessment.json")
            self.assertFalse(assessment["constraints_satisfied"])
            self.assertTrue(assessment["missing_checkpoints"])

    async def test_gate_timeout_is_recorded_and_does_not_claim_reproduction(self):
        async def hang(*args, **kwargs):
            await asyncio.Event().wait()
        selected = config()
        selected.schedule.gate_timeout_seconds = 0.01
        with tempfile.TemporaryDirectory() as tmp, patch.object(SharedModel, "generate", hang):
            path, passed = await run(selected, tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"])
            self.assertEqual(read(path, "manifest.json")["status"], "timed_out")
            self.assertIn("schedule_gate_timeout", read(path, "private/outcome_assessment.json")["infrastructure_failures"])
            self.assertFalse(read(path, "private/schedule_assessment.json")["constraints_satisfied"])

    async def test_activated_fault_with_model_failure_has_unknown_outcome(self):
        from testbed.schema import OrderDecision
        async def fail_on_b(model, body, agent_id, step_id, fixture, evidence):
            if agent_id == "agent_updater_B":
                raise ModelFailure("malformed_model_output")
            return OrderDecision(order_id="order_A", quantity=3)
        with tempfile.TemporaryDirectory() as tmp, patch.object(SharedModel, "generate", fail_on_b):
            path, passed = await run(config("stage4_stale_state.yaml"), tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"])
            self.assertEqual(read(path, "private/injection_manifest.json")["status"], "activated_outcome_unknown")

    async def test_live_requests_and_replay_are_mock_only_and_schedule_sensitive(self):
        async def mock(request):
            if request.url.path == "/api/version":
                return httpx.Response(200, json={"version": "mock"})
            if request.url.path == "/api/tags":
                return httpx.Response(200, json={"models": [{"name": "qwen3:8b", "digest": "mock-digest"}]})
            body = json.loads(request.content)
            user = json.loads(body["messages"][1]["content"])
            self.assertEqual(set(user), {"order_id", "document", "state"})
            self.assertNotIn("writer_event_id", user["state"])
            self.assertIn("Copy order_id exactly", body["messages"][0]["content"])
            self.assertIn("order_id", body["format"]["properties"])
            quantity = 3 if user["order_id"] == "order_A" else 4
            return httpx.Response(200, json={"done": True, "done_reason": "stop", "message": {"content": json.dumps({"order_id": user["order_id"], "quantity": quantity})}})
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
            changed = config("stage4_lost_update.yaml")
            changed.experiment.mode = "recorded_response"
            changed.experiment.replay_directory = str(source)
            path, passed = await run(changed, tmp)
            self.assertFalse(passed)
            self.assertIn("recorded_response_mismatch", read(path, "private/outcome_assessment.json")["model_outcomes"])

    def test_incompatible_fault_schedule_and_retry_settings_are_rejected(self):
        data = config("stage4_lost_update.yaml").model_dump()
        data["workflow"]["state_write_policy"] = "compare_and_set"
        with self.assertRaises(ValidationError):
            Config.model_validate(data)
        data = config().model_dump()
        data["workflow"]["max_additional_retries"] = 1
        with self.assertRaises(ValidationError):
            Config.model_validate(data)
