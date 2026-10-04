import asyncio
import json
import time
from pathlib import Path
import httpx
from .recording import digest
from .schema import Facts, OrderDecision


class ModelFailure(Exception):
    """Expected model or replay outcome, never silently regenerated."""


class SharedModel:
    def __init__(self, config, mode, replay_directory=None, transport=None):
        self.config = config
        self.mode = mode
        self.slot = asyncio.Semaphore(config.inference_slots)
        self.client = httpx.AsyncClient(base_url=config.base_url, timeout=600, transport=transport)
        self.model_digest = "scripted-fixture-v1" if mode == "scripted_fixture" else None
        self.ollama_version = None
        self.records = {}
        if mode == "recorded_response":
            source = Path(replay_directory)
            manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
            if manifest.get("execution_mode") != "live":
                raise ValueError("recorded_response must use a live source run")
            self.model_digest = manifest["model_digest"]
            if not self.model_digest:
                raise ValueError("source run has no model digest")
            self.ollama_version = manifest.get("ollama_version")
            for line in (source / "model_responses.jsonl").read_text(encoding="utf-8").splitlines():
                record = json.loads(line)
                key = (record["agent_id"], record["step_id"], record["request_hash"])
                if key in self.records:
                    raise ValueError("ambiguous duplicate replay request")
                self.records[key] = record["response"]

    async def initialize(self):
        if self.mode != "live":
            return
        version = await self.client.get("/api/version")
        version.raise_for_status()
        self.ollama_version = version.json()["version"]
        tags = await self.client.get("/api/tags")
        tags.raise_for_status()
        match = next((m for m in tags.json()["models"] if m["name"] == self.config.name), None)
        if not match or not match.get("digest"):
            raise RuntimeError("qwen3:8b must already be installed, with a discoverable digest")
        self.model_digest = match["digest"]

    def request(self, messages, seed, response_model=Facts):
        return {"model": self.config.name, "messages": messages, "stream": False,
                "think": self.config.think, "format": response_model.model_json_schema(),
                "options": {"num_ctx": self.config.num_ctx, "num_predict": self.config.num_predict,
                            "temperature": self.config.temperature, "seed": seed}}

    def request_hash(self, body):
        return digest({"request": body, "model_digest": self.model_digest})

    async def generate(self, body, agent_id, step_id, fixture, evidence):
        queued = time.perf_counter_ns()
        acquired = False
        started = None
        try:
            await self.slot.acquire()
            acquired = True
            started = time.perf_counter_ns()
            evidence["queue_duration_ns"] = started - queued
            if self.mode == "live":
                response = await self.client.post("/api/chat", json=body)
                response.raise_for_status()
                raw = response.json()
            elif self.mode == "recorded_response":
                key = (agent_id, step_id, self.request_hash(body))
                if key not in self.records:
                    raise ModelFailure("recorded_response_mismatch")
                raw = self.records[key]
            else:
                await asyncio.sleep(0)  # yield to other agents; not a controlled schedule
                raw = {"message": {"role": "assistant", "content": json.dumps(fixture)},
                       "done": True, "done_reason": "stop", "prompt_eval_count": None, "eval_count": None}
            evidence["response"] = raw
            evidence["prompt_tokens"] = raw.get("prompt_eval_count")
            evidence["generated_tokens"] = raw.get("eval_count")
            if raw.get("error"):
                raise ModelFailure("model_error: " + str(raw["error"]))
            if not raw.get("done") or raw.get("done_reason") == "length":
                raise ModelFailure("response_truncated")
            if raw.get("prompt_eval_count", 0) is not None and raw.get("prompt_eval_count", 0) >= self.config.num_ctx:
                raise ModelFailure("context_limit_reached_or_overflow")
            message = raw.get("message", {})
            if message.get("thinking"):
                raise ModelFailure("thinking_not_disabled")
            try:
                response_model = next((schema for schema in (Facts, OrderDecision)
                                       if schema.model_json_schema() == body["format"]), None)
                if response_model is None:
                    raise ValueError("unsupported response schema")
                return response_model.model_validate_json(message["content"])
            except (ValueError, KeyError) as exc:
                raise ModelFailure("malformed_model_output") from exc
        except httpx.HTTPStatusError as exc:
            evidence["http_status"] = exc.response.status_code
            evidence["error_response"] = exc.response.text
            if "context" in exc.response.text.lower():
                raise ModelFailure("context_overflow") from exc
            raise
        finally:
            now = time.perf_counter_ns()
            if started is None:
                evidence["queue_duration_ns"] = now - queued
            else:
                evidence["request_duration_ns"] = now - started
            if acquired:
                self.slot.release()

    async def close(self):
        await self.client.aclose()
