import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from .schema import DependencyRef, Event, PayloadRef


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def save_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")


class Recorder:
    def __init__(self, directory, run_id):
        self.directory = directory
        self.run_id = run_id
        (directory / "payloads").mkdir()
        (directory / "events.jsonl").touch()
        self.events = []
        self.sequences = {}
        self.previous = {}

    def payload(self, value):
        ref = PayloadRef(payload_id=uuid4().hex, sha256=digest(value))
        (self.directory / "payloads" / f"{ref.payload_id}.json").write_bytes(canonical(value))
        return ref

    def emit(self, actor, step, kind, status, operation, attempt, inputs=(), outputs=(), dependencies=(), **details):
        sequence = self.sequences.get(actor, 0) + 1
        deps = list(dependencies)
        if actor in self.previous:
            deps.append(DependencyRef(event_id=self.previous[actor], relationship="actor_local_order"))
        event = Event(run_id=self.run_id, event_id=uuid4().hex,
                      agent_id=actor if actor.startswith("agent_") else None,
                      component_id=None if actor.startswith("agent_") else actor,
                      step_id=step, event_type=kind, status=status,
                      local_sequence=sequence, monotonic_ns=time.perf_counter_ns(), wall_time_utc=utc_now(),
                      operation_id=operation, attempt_id=attempt, input_refs=list(inputs), output_refs=list(outputs),
                      dependency_refs=deps, details=details)
        with (self.directory / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(event.model_dump_json() + "\n")
        self.events.append(event)
        self.sequences[actor] = sequence
        self.previous[actor] = event.event_id
        return event


def validate_trace(directory):
    errors, events, known, sequences, operations = [], [], set(), {}, {}
    try:
        for line in (directory / "events.jsonl").read_text(encoding="utf-8").splitlines():
            events.append(Event.model_validate_json(line))
    except Exception as exc:
        return {"valid": False, "errors": [str(exc)]}
    run_ids = {e.run_id for e in events}
    if len(run_ids) != 1:
        errors.append("trace must contain exactly one run")
    for e in events:
        actor = e.agent_id or e.component_id
        if e.event_id in known:
            errors.append("duplicate event identifier")
        if e.local_sequence != sequences.get(actor, 0) + 1:
            errors.append("actor sequence gap")
        sequences[actor] = e.local_sequence
        for dep in e.dependency_refs:
            if dep.event_id not in known:
                errors.append(f"broken dependency {dep.event_id}")
        for ref in e.input_refs + e.output_refs:
            try:
                value = json.loads((directory / "payloads" / f"{ref.payload_id}.json").read_text(encoding="utf-8"))
                if digest(value) != ref.sha256:
                    errors.append(f"payload hash mismatch {ref.payload_id}")
            except Exception:
                errors.append(f"missing/invalid payload {ref.payload_id}")
        key = (e.operation_id, e.attempt_id)
        operations.setdefault(key, []).append(e.status)
        known.add(e.event_id)
    for statuses in operations.values():
        if len(statuses) != 2 or statuses[0] != "started" or statuses[1] == "started":
            errors.append("operation missing start or terminal event")
    return {"valid": not errors, "event_count": len(events), "errors": errors}
