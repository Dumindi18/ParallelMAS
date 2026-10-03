import asyncio
import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
import httpx
from pydantic import ValidationError
from testbed.__main__ import run
from testbed.model import SharedModel, ModelFailure
from testbed.recording import validate_trace, save_json
from testbed.evaluation import check_outcome
from testbed.schema import Config, ModelConfig


class Stage2Tests(unittest.IsolatedAsyncioTestCase):
    async def test_document_identity_is_supplied_and_recorded_for_both_agents(self):
        seen = {}
        async def extract_from_prompt(model, body, agent_id, step_id, fixture, evidence):
            from testbed.schema import Facts
            document = json.loads(body["messages"][1]["content"])
            self.assertEqual(set(document), {"document_id", "document"})
            self.assertEqual(document["document_id"], "department_A")
            self.assertIn("7 notebooks", document["document"])
            self.assertIn("Copy document_id exactly", body["messages"][0]["content"])
            seen[agent_id] = document
            # Derive identity from the actual request, independently of fixture answers.
            return Facts(document_id=document["document_id"], quantity=7, unit_cost_cents=125)
        with tempfile.TemporaryDirectory() as tmp, patch.object(SharedModel, "generate", extract_from_prompt):
            path, passed = await run(Config(experiment={"name": "identity", "task_id": "orders_001"}), tmp)
            self.assertTrue(passed)
            self.assertEqual(set(seen), {"agent_extractor", "agent_checker"})
            manifest = json.loads((path / "manifest.json").read_text())
            self.assertEqual(manifest["prompt_versions"]["extract_facts"], "1.1")
            events = [json.loads(line) for line in (path / "events.jsonl").read_text().splitlines()]
            for event in events:
                if event["event_type"] == "model_request" and event["status"] == "started":
                    ref = event["input_refs"][0]
                    request = json.loads((path / "payloads" / f"{ref['payload_id']}.json").read_text())
                    self.assertEqual(json.loads(request["messages"][1]["content"]), seen[event["agent_id"]])

    async def test_failed_model_run_preserves_valid_trace_and_separate_outcome(self):
        async def fail(*args, **kwargs):
            raise ModelFailure("malformed_model_output")
        with tempfile.TemporaryDirectory() as tmp, patch.object(SharedModel, "generate", fail):
            path, passed = await run(Config(experiment={"name": "failure", "task_id": "orders_001"}), tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"])
            assessment = json.loads((path / "private" / "outcome_assessment.json").read_text())
            self.assertEqual(assessment["task_correctness"], "unknown")
            self.assertEqual(assessment["model_outcomes"], ["malformed_model_output"])
            self.assertEqual(assessment["infrastructure_failures"], [])

    async def test_run_timeout_closes_cancelled_operations(self):
        async def hang(*args, **kwargs):
            await asyncio.Event().wait()
        with tempfile.TemporaryDirectory() as tmp, patch.object(SharedModel, "generate", hang):
            config = Config(experiment={"name": "timeout", "task_id": "orders_001"},
                            runtime={"run_timeout_seconds": 0.05})
            path, passed = await run(config, tmp)
            self.assertFalse(passed)
            self.assertTrue(validate_trace(path)["valid"])
            manifest = json.loads((path / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "timed_out")

    async def test_complete_fixture_without_network_and_payload_integrity(self):
        def forbidden(request):
            raise AssertionError("fixture must never use HTTP")
        model = SharedModel(ModelConfig(), "scripted_fixture", transport=httpx.MockTransport(forbidden))
        await model.initialize()
        evidence = {}
        body = model.request([], 42)
        await model.generate(body, "agent_test", "facts", {"document_id": "a", "quantity": 1, "unit_cost_cents": 2}, evidence)
        await model.close()
        with tempfile.TemporaryDirectory() as tmp:
            path, passed = await run(Config(experiment={"name": "test", "task_id": "orders_001"}), tmp)
            self.assertTrue(passed)
            output = json.loads((path / "final_output.json").read_text())
            self.assertEqual(output["total_cost_cents"], 875)
            events = [json.loads(line) for line in (path / "events.jsonl").read_text().splitlines()]
            starts = {e["agent_id"]: e["monotonic_ns"] for e in events if e["event_type"] == "model_request" and e["status"] == "started"}
            ends = {e["agent_id"]: e["monotonic_ns"] for e in events if e["event_type"] == "model_request" and e["status"] == "completed"}
            self.assertLess(max(starts.values()), min(ends.values()))
            self.assertTrue(all("queue_duration_ns" in e["details"] and "request_duration_ns" in e["details"] for e in events if e["event_type"] == "model_request" and e["status"] == "completed"))
            payload = next((path / "payloads").glob("*.json"))
            payload.write_text('{"tampered":true}')
            self.assertFalse(validate_trace(path)["valid"])

    async def test_invalid_output_is_not_repaired(self):
        model = SharedModel(ModelConfig(), "scripted_fixture")
        try:
            with self.assertRaisesRegex(ModelFailure, "malformed_model_output"):
                await model.generate(model.request([], 42), "a", "s", {"quantity": "bad"}, {})
        finally:
            await model.close()

    async def test_live_wrapper_with_mock_http_only(self):
        calls = []
        async def mock(request):
            calls.append(request.url.path)
            if request.url.path == "/api/version":
                return httpx.Response(200, json={"version": "test"})
            if request.url.path == "/api/tags":
                return httpx.Response(200, json={"models": [{"name": "qwen3:8b", "digest": "exact-digest"}]})
            body = json.loads(request.content)
            self.assertFalse(body["think"])
            self.assertFalse(body["stream"])
            self.assertIn("properties", body["format"])
            return httpx.Response(200, json={"done": True, "done_reason": "length", "message": {"content": "{}"}})
        model = SharedModel(ModelConfig(), "live", transport=httpx.MockTransport(mock))
        try:
            await model.initialize()
            self.assertEqual(model.model_digest, "exact-digest")
            with self.assertRaisesRegex(ModelFailure, "response_truncated"):
                await model.generate(model.request([], 42), "a", "s", {}, {})
            self.assertEqual(calls.count("/api/chat"), 1)
        finally:
            await model.close()

    async def test_replay_requires_exact_request_settings_and_step(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            save_json(path / "manifest.json", {"execution_mode": "live", "model_digest": "exact"})
            (path / "model_responses.jsonl").touch()
            initial = SharedModel(ModelConfig(), "recorded_response", tmp)
            body = initial.request([], 42)
            request_hash = initial.request_hash(body)
            await initial.close()
            record = {"agent_id": "a", "step_id": "s", "request_hash": request_hash,
                      "response": {"done": True, "message": {"content": '{"document_id":"a","quantity":1,"unit_cost_cents":2}'}}}
            (path / "model_responses.jsonl").write_text(json.dumps(record) + "\n")
            model = SharedModel(ModelConfig(), "recorded_response", tmp)
            try:
                result = await model.generate(body, "a", "s", {}, {})
                self.assertEqual(result.quantity, 1)
                changed = model.request([], 43)
                with self.assertRaisesRegex(ModelFailure, "recorded_response_mismatch"):
                    await model.generate(changed, "a", "s", {}, {})
                with self.assertRaisesRegex(ModelFailure, "recorded_response_mismatch"):
                    await model.generate(body, "a", "other", {}, {})
            finally:
                await model.close()

    def test_checker_and_config_validation(self):
        task = {"document_id": "a", "requirements": {"quantity": 1, "unit_cost_cents": 2, "total_cost_cents": 2}}
        output = {"document_id": "a", "quantity": 1, "unit_cost_cents": 2, "total_cost_cents": 2, "approved": False}
        self.assertEqual(check_outcome(task, output)["task_correctness"], "failure")
        self.assertEqual(check_outcome(task, None)["task_correctness"], "unknown")
        with self.assertRaises(ValidationError):
            ModelConfig(think=True)
        with self.assertRaises(ValidationError):
            Config(experiment={"name": "t", "task_id": "orders_001", "mode": "recorded_response"})


if __name__ == "__main__":
    unittest.main()
