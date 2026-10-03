"""Stage 3 transport, message use and joins; no causal diagnosis."""
import asyncio
import json
from dataclasses import dataclass
from uuid import uuid4
from .runtime import Agent, Runtime
from .schema import DependencyRef, Message
from .faults import JoinFaultController


@dataclass
class Receipt:
    message: Message
    send_event_id: str
    delivery_event_id: str
    receive_event_id: str | None = None


class JoinTimeout(TimeoutError):
    pass


class ParallelRuntime(Runtime):
    def __init__(self, config, recorder, model):
        super().__init__(config, recorder, model)
        self.agents = {}
        self.deliveries = []
        self.received = {}
        self.consumed = set()
        self.completed_branches = set()
        self.delivered_branches = set()
        self.aggregation_finished = asyncio.Event()
        self.fault_controller = JoinFaultController(config)
        self.injection = self.fault_controller.record

    def register(self, *agents):
        self.agents.update({agent.agent_id: agent for agent in agents})

    def value(self, message):
        path = self.recorder.directory / "payloads" / f"{message.payload_reference.payload_id}.json"
        return json.loads(path.read_text(encoding="utf-8"))

    async def send_message(self, agent, receiver_id, value, source_event_id):
        if receiver_id not in self.agents:
            raise ValueError("unknown message recipient")
        message = Message(message_id=uuid4().hex, sender_id=agent.agent_id, receiver_id=receiver_id,
                          payload_reference=self.recorder.payload(value), source_event_id=source_event_id)
        async def send(evidence):
            evidence["_input_refs"] = [message.payload_reference]
            return message
        _, event, _ = await self.action(agent, "send_result", "message_sent", message.model_dump(), send,
            [DependencyRef(event_id=source_event_id, relationship="produced_output")], message.model_dump())
        self.deliveries.append(asyncio.create_task(self._deliver(message, event.event_id)))
        return message

    async def _deliver(self, message, send_event_id):
        # Transport components are independent actors, not workflow agents.
        transport = Agent(f"transport_{message.sender_id}", "message delivery")
        async def deliver(evidence):
            if message.sender_id == self.config.workflow.hold_branch_until_aggregation:
                await self.aggregation_finished.wait()
            delay = self.config.workflow.message_delays.get(message.sender_id, 0)
            if delay:
                await asyncio.sleep(delay)  # optional natural transport latency, not a schedule gate
            evidence["_input_refs"] = [message.payload_reference]
            return message
        _, event, _ = await self.action(transport, "deliver_result", "message_delivered", message.model_dump(), deliver,
            [DependencyRef(event_id=send_event_id, relationship="message_send_delivery")],
            {**message.model_dump(), "send_event_id": send_event_id})
        # No await between delivery evidence and inbox insertion: the recorded delivery makes it available.
        self.delivered_branches.add(message.sender_id)
        self.agents[message.receiver_id].inbox.put_nowait(Receipt(message, send_event_id, event.event_id))

    async def receive_message(self, agent):
        receipt = None
        async def receive(evidence):
            nonlocal receipt
            receipt = await agent.inbox.get()
            message = receipt.message
            evidence.update(**message.model_dump(), send_event_id=receipt.send_event_id,
                            delivery_event_id=receipt.delivery_event_id)
            evidence["_dependencies"] = [DependencyRef(event_id=receipt.delivery_event_id, relationship="message_delivery_receive")]
            evidence["_input_refs"] = [message.payload_reference]
            return message
        _, event, _ = await self.action(agent, "receive_result", "message_received", {}, receive)
        receipt.receive_event_id = event.event_id
        self.received[receipt.message.message_id] = receipt
        return receipt

    async def consume_message(self, agent, receipt, step):
        message = receipt.message
        if self.received.get(message.message_id) is not receipt or message.receiver_id != agent.agent_id:
            raise ValueError("message must be received by its owner before consumption")
        if message.message_id in self.consumed:
            raise ValueError("worker result cannot be counted twice")
        async def consume(evidence):
            evidence["_input_refs"] = [message.payload_reference]
            self.consumed.add(message.message_id)
            return self.value(message)
        return await self.action(agent, step, "message_consumed", message.model_dump(), consume,
            [DependencyRef(event_id=receipt.delivery_event_id, relationship="message_delivery_consumption"),
             DependencyRef(event_id=receipt.receive_event_id, relationship="produced_output")],
            {**message.model_dump(), "send_event_id": receipt.send_event_id,
             "delivery_event_id": receipt.delivery_event_id, "receive_event_id": receipt.receive_event_id,
             "consuming_step_id": "aggregate_orders"})

    async def wait_for_join(self, agent, required):
        join_id = "department_orders"
        accepted = []
        seen = set()
        async def join(evidence):
            threshold = self.fault_controller.join_release_threshold(join_id, required)
            try:
                async with asyncio.timeout(self.config.workflow.join_timeout_seconds):
                    while len(accepted) < threshold:
                        receipt = await self.receive_message(agent)
                        branch = receipt.message.sender_id
                        if branch not in required or branch in seen:
                            raise ValueError("unexpected or duplicate join branch")
                        seen.add(branch)
                        accepted.append(receipt)
            except TimeoutError as exc:
                evidence["release_reason"] = "timeout"
                evidence["timeout_status"] = True
                raise JoinTimeout("join_timeout: department_orders") from exc
            finally:
                evidence.update(completed_branches=sorted(self.completed_branches),
                                available_branches=sorted(seen | self.delivered_branches),
                                accepted_result_ids=[r.message.message_id for r in accepted],
                                missing_branches=[branch for branch in required if branch not in seen])
                evidence["_input_refs"] = [r.message.payload_reference for r in accepted]
                evidence["_dependencies"] = [DependencyRef(event_id=r.message.source_event_id, relationship="accepted_branch_result") for r in accepted]
            missing = evidence["missing_branches"]
            evidence["release_reason"] = "premature_release" if self.config.fault.enabled else "all_required_inputs"
            evidence["timeout_status"] = False
            self.fault_controller.record_release(join_id, required, seen)
            return {"join_id": join_id, "required_branches": required,
                    **{key: value for key, value in evidence.items() if not key.startswith("_")}}
        _, event, _ = await self.action(agent, "join_worker_results", "join", {"required_branches": required}, join,
            details={"join_id": join_id, "required_branches": required})
        if self.injection["activated"]:
            self.injection["injection_event"] = event.event_id
        return accepted, event

    async def drain_deliveries(self):
        if self.deliveries:
            await asyncio.gather(*self.deliveries)

    async def close_deliveries(self):
        for task in self.deliveries:
            if not task.done():
                task.cancel()
        if self.deliveries:
            await asyncio.gather(*self.deliveries, return_exceptions=True)


async def parallel_workflow(runtime, task):
    workers = [Agent(f"agent_worker_{index + 1}", "You extract order facts from your assigned departmental document.")
               for index in range(len(task["documents"]))]
    aggregator = Agent("agent_aggregator", "You aggregate required worker results exactly once.")
    runtime.register(*workers, aggregator)
    async def worker(agent, document, fixture):
        data, event, _ = await runtime.call_tool(agent, "read_document", "local_document",
            {"document_id": document["document_id"]}, lambda _: document)
        facts, output_event, _ = await runtime.call_model(agent, "extract_facts", data, fixture,
            [DependencyRef(event_id=event.event_id, relationship="produced_output")])
        runtime.completed_branches.add(agent.agent_id)
        await runtime.send_message(agent, aggregator.agent_id, facts.model_dump(), output_event.event_id)
        agent.status = "completed"

    async def aggregate():
        receipts, join_event = await runtime.wait_for_join(aggregator, [worker.agent_id for worker in workers])
        values, dependencies = [], [DependencyRef(event_id=join_event.event_id, relationship="produced_output")]
        for receipt in receipts:
            value, event, _ = await runtime.consume_message(aggregator, receipt, "use_worker_result")
            values.append({"message_id": receipt.message.message_id, "branch_id": receipt.message.sender_id, "facts": value})
            dependencies.append(DependencyRef(event_id=event.event_id, relationship="produced_output"))
        def total(arguments):
            results = arguments["results"]
            return {"results": results, "included_document_ids": [r["facts"]["document_id"] for r in results],
                    "accepted_result_ids": [r["message_id"] for r in results],
                    "total_quantity": sum(r["facts"]["quantity"] for r in results),
                    "total_cost_cents": sum(r["facts"]["quantity"] * r["facts"]["unit_cost_cents"] for r in results)}
        result, _, _ = await runtime.call_tool(aggregator, "aggregate_orders", "integer_order_aggregation",
            {"results": values}, total, dependencies)
        aggregator.status = "completed"
        runtime.aggregation_finished.set()
        return result

    # Start all branches and the aggregator together; a worker failure cancels waiting siblings.
    jobs = [asyncio.create_task(worker(agent, document, fixture))
            for agent, document, fixture in zip(workers, task["documents"], task["fixture_facts"])]
    aggregator_job = asyncio.create_task(aggregate())
    jobs.append(aggregator_job)
    try:
        await asyncio.gather(*jobs)
        await runtime.drain_deliveries()
        return aggregator_job.result(), [*workers, aggregator]
    except BaseException:
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        raise
    finally:
        await runtime.close_deliveries()
