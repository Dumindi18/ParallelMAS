import asyncio
from dataclasses import dataclass, field
from uuid import uuid4
from .schema import DependencyRef
from .recording import canonical

PROMPT_VERSION = "1.1"


@dataclass
class Agent:
    agent_id: str
    role: str
    context: list = field(default_factory=list)
    inbox: asyncio.Queue = field(default_factory=asyncio.Queue, repr=False)
    current_step: str | None = None
    local_sequence: int = 0
    actions: int = 0
    status: str = "created"


class Runtime:
    """Agents use these wrappers; Stage 3 will add transport and joins."""
    def __init__(self, config, recorder, model):
        self.config, self.recorder, self.model = config, recorder, model

    async def action(self, agent, step, kind, inputs, function, dependencies=(), details=None):
        if agent.actions >= self.config.runtime.max_steps_per_agent:
            raise RuntimeError("agent_step_limit_exceeded")
        agent.actions += 1
        agent.current_step, agent.status = step, "running"
        operation, attempt = uuid4().hex, uuid4().hex
        input_ref = self.recorder.payload(inputs)
        evidence = dict(details or {})
        begin = self.recorder.emit(agent.agent_id, step, kind, "started", operation, attempt,
                                   inputs=[input_ref], dependencies=dependencies, **evidence)
        dependencies = [*dependencies, DependencyRef(event_id=begin.event_id, relationship="operation_start")]
        try:
            result = await function(evidence)
            dependencies.extend(evidence.pop("_dependencies", []))
            extra_inputs = evidence.pop("_input_refs", [])
            value = result.model_dump() if hasattr(result, "model_dump") else result
            output = self.recorder.payload(value)
            evidence_refs = []
            if "response" in evidence:
                raw = self.recorder.payload(evidence.pop("response"))
                evidence["response_ref"] = raw.model_dump()
                evidence_refs.append(raw)
            end = self.recorder.emit(agent.agent_id, step, kind, "completed", operation, attempt,
                                     inputs=[input_ref, *extra_inputs], outputs=[output, *evidence_refs], dependencies=dependencies, **evidence)
            return result, end, output
        except BaseException as exc:
            status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "timed_out" if isinstance(exc, TimeoutError) else "failed"
            dependencies.extend(evidence.pop("_dependencies", []))
            extra_inputs = evidence.pop("_input_refs", [])
            outputs = []
            if "response" in evidence:
                outputs.append(self.recorder.payload(evidence.pop("response")))
            self.recorder.emit(agent.agent_id, step, kind, status, operation, attempt,
                               inputs=[input_ref, *extra_inputs], outputs=outputs, dependencies=dependencies,
                               error_type=type(exc).__name__, error=str(exc), **evidence)
            agent.status = status
            raise
        finally:
            agent.local_sequence = self.recorder.sequences[agent.agent_id]

    async def call_tool(self, agent, step, name, arguments, tool, dependencies=()):
        async def invoke(evidence):
            return tool(arguments)
        return await self.action(agent, step, "tool_call", arguments, invoke, dependencies,
                                 {"tool_name": name, "tool_version": "1.0"})

    async def call_model(self, agent, step, document, fixture, dependencies=()):
        agent.context = [{"role": "system", "content": agent.role + " Return only JSON with document_id, quantity, unit_cost_cents. The input contains document_id metadata and document text. Copy document_id exactly from the metadata; do not infer or rename it. Extract quantity and unit_cost_cents from the document text; do not invent values."},
                         {"role": "user", "content": canonical(document).decode("utf-8")}]
        body = self.model.request(agent.context, self.config.experiment.seed)
        async def invoke(evidence):
            return await self.model.generate(body, agent.agent_id, step, fixture, evidence)
        result = await self.action(agent, step, "model_request", body, invoke, dependencies,
                                  {"model_digest": self.model.model_digest,
                                   "request_hash": self.model.request_hash(body),
                                   "settings": body["options"], "think": body["think"],
                                   "execution_mode": self.config.experiment.mode})
        # Capture the exact validated request/response for future matching, not a semantic cache.
        end = result[1]
        response_ref = end.details.get("response_ref")
        if response_ref:
            import json
            response = json.loads((self.recorder.directory / "payloads" / f"{response_ref['payload_id']}.json").read_text(encoding="utf-8"))
            record = {"agent_id": agent.agent_id, "step_id": step,
                      "request_hash": self.model.request_hash(body), "response": response}
            with (self.recorder.directory / "model_responses.jsonl").open("ab") as stream:
                stream.write(canonical(record) + b"\n")
        return result


async def workflow(runtime, task):
    extractor = Agent("agent_extractor", "You are the departmental order fact extractor.")
    checker = Agent("agent_checker", "You independently check departmental order facts.")
    executor = Agent("agent_executor", "You calculate the approved order total using deterministic arithmetic.")
    fixture = {"document_id": task["document_id"], "quantity": task["requirements"]["quantity"],
               "unit_cost_cents": task["requirements"]["unit_cost_cents"]}

    async def inspect(agent):
        document, event, ref = await runtime.call_tool(agent, "read_document", "local_document",
            {"document_id": task["document_id"]},
            lambda _: {"document_id": task["document_id"], "document": task["document"]})
        facts, end, output_ref = await runtime.call_model(agent, "extract_facts", document, fixture,
            [DependencyRef(event_id=event.event_id, relationship="produced_output")])
        agent.status = "completed"
        return facts, end, output_ref

    branches = [asyncio.create_task(inspect(a)) for a in [extractor, checker]]
    try:
        extracted, checked = await asyncio.gather(*branches)
    except BaseException:
        for branch in branches:
            branch.cancel()
        await asyncio.gather(*branches, return_exceptions=True)
        raise
    def calculate(arguments):
        approved = arguments["extracted"] == arguments["checked"]
        return {"document_id": arguments["extracted"]["document_id"], "approved": approved,
                "quantity": arguments["extracted"]["quantity"],
                "unit_cost_cents": arguments["extracted"]["unit_cost_cents"],
                "total_cost_cents": arguments["extracted"]["quantity"] * arguments["extracted"]["unit_cost_cents"] if approved else None}
    result, _, _ = await asyncio.create_task(runtime.call_tool(executor, "calculate_approved_total", "integer_order_total",
        {"extracted": extracted[0].model_dump(), "checked": checked[0].model_dump()}, calculate,
        [DependencyRef(event_id=value[1].event_id, relationship="produced_output") for value in [extracted, checked]]))
    executor.status = "completed"
    return result, [extractor, checker, executor]
