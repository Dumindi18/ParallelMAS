"""Template C: two independent order updaters and one deterministic verifier."""
import asyncio
from .faults import StateFaultController
from .recording import canonical
from .runtime import Agent, Runtime
from .schema import DependencyRef, OrderDecision
from .scheduling import Scheduler
from .state import VersionedState
from .model import ModelFailure

ORDER_PROMPT_VERSION = "1.0"


class SharedStateRuntime(Runtime):
    def __init__(self, config, recorder, model):
        super().__init__(config, recorder, model)
        self.state = VersionedState()
        self.scheduler = Scheduler(config.schedule, recorder)
        self.fault_controller = StateFaultController(config)
        self.injection = self.fault_controller.record
        self.key = None

    async def initialize_state(self, key, value):
        self.key = key
        component = Agent("state_service", "initial state")
        async def initialize(evidence):
            record = await self.state.initialize(key, value, evidence["_terminal_event_id"])
            evidence.update(record)
            return record
        return await self.action(component, "initialize_inventory", "state_initialized",
                                 {"key": key, "value": value}, initialize)

    async def read_state(self, agent, step="read_inventory", dependencies=()):
        async def invoke(evidence):
            version = self.fault_controller.read_version(agent.agent_id, self.key)
            result = await self.state.read(self.key, version)
            record, current = result["record"], result["current_record"]
            evidence.update(key=self.key, returned_version=record["version"], returned_value=record["value"],
                            originating_write_event_id=record["writer_event_id"], current_version=current["version"],
                            current_writer_event_id=current["writer_event_id"])
            evidence["_dependencies"] = [DependencyRef(event_id=record["writer_event_id"], relationship="state_write_read")]
            self.fault_controller.record_read(agent.agent_id, self.key, result, evidence["_terminal_event_id"])
            return result
        async def action(gate_id):
            return await self.action(agent, step, "state_read", {"key": self.key}, invoke,
                [*dependencies, DependencyRef(event_id=gate_id, relationship="schedule_order")])
        return await self.scheduler.execute(agent, step, action)

    async def write_state(self, agent, proposal, read_event, proposal_event):
        async def invoke(evidence):
            result = await self.state.write(self.key, proposal["value"], proposal["base_version"],
                                             self.config.workflow.state_write_policy, evidence["_terminal_event_id"])
            evidence.update(result)
            evidence["_dependencies"] = [DependencyRef(event_id=result["previous_writer_event_id"], relationship="state_previous_write")]
            self.fault_controller.record_write(agent.agent_id, self.key, result, evidence["_terminal_event_id"])
            return result
        async def action(gate_id):
            return await self.action(agent, "write_inventory", "state_write", proposal, invoke,
                [DependencyRef(event_id=read_event.event_id, relationship="state_read_write"),
                 DependencyRef(event_id=proposal_event.event_id, relationship="produced_output"),
                 DependencyRef(event_id=gate_id, relationship="schedule_order")])
        return await self.scheduler.execute(agent, "write_inventory", action)


async def shared_record_workflow(runtime, task):
    initialized, _, _ = await runtime.initialize_state(task["key"], task["initial_value"])
    agents = [Agent("agent_updater_A", "You extract the requested reservation from an order document."),
              Agent("agent_updater_B", "You independently extract the requested reservation from an order document.")]
    verifier = Agent("agent_verifier", "You inspect the final inventory record and accepted reservation evidence.")

    async def update(agent, order, fixture):
        snapshot, read_event, _ = await runtime.read_state(agent)
        record = snapshot["record"]
        model_input = {"order_id": order["order_id"], "document": order["document"],
                       "state": {"key": record["key"], "version": record["version"], "value": record["value"]}}
        messages = [{"role": "system", "content": agent.role + " Return only JSON with order_id and quantity. Copy order_id exactly from the provided metadata. Extract the requested quantity from document text. Do not invent values or change the order identifier."},
                    {"role": "user", "content": canonical(model_input).decode("utf-8")}]
        decision, decision_event, _ = await runtime.call_structured_model(agent, "interpret_order", messages,
            fixture, OrderDecision, [DependencyRef(event_id=read_event.event_id, relationship="produced_output")])

        def propose(arguments):
            current, selected = arguments["snapshot"], arguments["decision"]
            if selected["order_id"] != order["order_id"]:
                raise ModelFailure("order_identity_mismatch")
            value = current["value"]
            if selected["quantity"] > value["available_quantity"]:
                raise ModelFailure("insufficient_inventory")
            if any(item["order_id"] == selected["order_id"] for item in value["accepted_orders"]):
                raise ModelFailure("duplicate_order")
            return {"key": current["key"], "base_version": current["version"],
                    "value": {"available_quantity": value["available_quantity"] - selected["quantity"],
                              "accepted_orders": [*value["accepted_orders"], selected]}}
        proposal, proposal_event, _ = await runtime.call_tool(agent, "prepare_reservation", "inventory_reservation",
            {"snapshot": record, "decision": decision.model_dump()}, propose,
            [DependencyRef(event_id=read_event.event_id, relationship="produced_output"),
             DependencyRef(event_id=decision_event.event_id, relationship="produced_output")])
        result, write_event, _ = await runtime.write_state(agent, proposal, read_event, proposal_event)
        agent.status = "completed"
        return {"agent_id": agent.agent_id, "order": decision.model_dump(),
                "read_version": record["version"], "current_version_at_read": snapshot["current_record"]["version"],
                "write_accepted": result["accepted"], "write_event_id": write_event.event_id,
                "rejection_reason": result["rejection_reason"]}

    jobs = [asyncio.create_task(update(agent, order, fixture))
            for agent, order, fixture in zip(agents, task["orders"], task["fixture_decisions"])]
    try:
        decisions = await asyncio.gather(*jobs)
    except BaseException:
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        raise

    async def verify():
        snapshot, read_event, _ = await runtime.read_state(verifier, "read_final_inventory")
        result, _, _ = await runtime.call_tool(verifier, "verify_inventory", "inventory_inspection",
            {"final_record": snapshot["record"], "updates": decisions}, lambda arguments: arguments,
            [DependencyRef(event_id=read_event.event_id, relationship="produced_output"),
             *[DependencyRef(event_id=item["write_event_id"], relationship="produced_output") for item in decisions]])
        verifier.status = "completed"
        return result
    output = await asyncio.create_task(verify())
    return output, [*agents, verifier]
