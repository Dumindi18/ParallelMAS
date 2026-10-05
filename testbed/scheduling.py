"""Named asynchronous gates; requested constraints are separate from observed evidence."""
import asyncio
from uuid import uuid4
from .schema import DependencyRef


PROFILES = {
    "natural": [],
    "a_then_b": ["agent_updater_A.read_inventory", "agent_updater_A.write_inventory",
                 "agent_updater_B.read_inventory", "agent_updater_B.write_inventory"],
    "b_then_a": ["agent_updater_B.read_inventory", "agent_updater_B.write_inventory",
                 "agent_updater_A.read_inventory", "agent_updater_A.write_inventory"],
    "both_read_before_writes": ["agent_updater_A.read_inventory", "agent_updater_B.read_inventory",
                                "agent_updater_A.write_inventory", "agent_updater_B.write_inventory"],
}


class ScheduleTimeout(TimeoutError):
    pass


class Scheduler:
    def __init__(self, config, recorder):
        self.config, self.recorder = config, recorder
        self.plan = list(PROFILES[config.profile])
        self.gates = {checkpoint: asyncio.Event() for checkpoint in self.plan}
        self.completed = {}
        self.observed = []

    async def execute(self, agent, step, action):
        checkpoint = f"{agent.agent_id}.{step}"
        actor = f"scheduler_{agent.agent_id}"
        previous = None
        if checkpoint in self.plan:
            index = self.plan.index(checkpoint)
            previous = self.plan[index - 1] if index else None
        operation, attempt = uuid4().hex, uuid4().hex
        started = self.recorder.emit(actor, checkpoint, "schedule_gate", "started", operation, attempt,
                                     checkpoint=checkpoint)
        deps = [DependencyRef(event_id=started.event_id, relationship="operation_start")]
        try:
            if previous:
                try:
                    await asyncio.wait_for(self.gates[previous].wait(), self.config.gate_timeout_seconds)
                except TimeoutError as exc:
                    raise ScheduleTimeout(f"schedule_gate_timeout: {checkpoint}") from exc
                deps.append(DependencyRef(event_id=self.completed[previous], relationship="schedule_order"))
        except BaseException as exc:
            status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "timed_out"
            self.recorder.emit(actor, checkpoint, "schedule_gate", status, operation, attempt,
                               dependencies=deps, checkpoint=checkpoint, error=str(exc))
            raise
        gate = self.recorder.emit(actor, checkpoint, "schedule_gate", "completed", operation, attempt,
                                  dependencies=deps, checkpoint=checkpoint)
        operation, attempt = uuid4().hex, uuid4().hex
        started = self.recorder.emit(actor, checkpoint, "schedule_checkpoint", "started", operation, attempt,
                                     dependencies=[DependencyRef(event_id=gate.event_id, relationship="schedule_order")],
                                     checkpoint=checkpoint)
        deps = [DependencyRef(event_id=started.event_id, relationship="operation_start")]
        try:
            result = await action(gate.event_id)
        except BaseException as exc:
            last_event = self.recorder.previous.get(agent.agent_id)
            if last_event:
                deps.append(DependencyRef(event_id=last_event, relationship="checkpoint_action"))
            status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "timed_out" if isinstance(exc, TimeoutError) else "failed"
            self.recorder.emit(actor, checkpoint, "schedule_checkpoint", status, operation, attempt,
                               dependencies=deps, checkpoint=checkpoint, error=str(exc))
            raise
        deps.append(DependencyRef(event_id=result[1].event_id, relationship="checkpoint_action"))
        event = self.recorder.emit(actor, checkpoint, "schedule_checkpoint", "completed", operation, attempt,
                                   dependencies=deps, checkpoint=checkpoint, action_event_id=result[1].event_id)
        self.completed[checkpoint] = event.event_id
        self.observed.append({"checkpoint": checkpoint, "event_id": event.event_id,
                              "action_event_id": result[1].event_id, "monotonic_ns": event.monotonic_ns})
        if checkpoint in self.gates:
            self.gates[checkpoint].set()
        return result

    def assessment(self):
        observed = [item["checkpoint"] for item in self.observed if item["checkpoint"] in self.plan]
        return {"mode": self.config.mode, "profile": self.config.profile,
                "planned_checkpoints": self.plan, "observed_checkpoints": observed,
                "constraints_satisfied": observed == self.plan if self.plan else True,
                "missing_checkpoints": [item for item in self.plan if item not in observed]}
