import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from .schema import DependencyRef, Event, PayloadRef, Message


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
    errors.extend(validate_message_evidence(directory, events))
    return {"valid": not errors, "event_count": len(events), "errors": errors}


def validate_message_evidence(directory, events):
    """Validate observed lifecycle and accepted inputs, never require consumption of late messages."""
    errors = []
    by_id = {event.event_id: event for event in events}
    sent, delivered, received, consumed = {}, {}, {}, {}
    joins, aggregation_inputs = [], []
    for event in events:
        if event.status != "completed":
            continue
        if event.event_type == "join":
            joins.append(event)
        if event.event_type == "tool_call" and event.step_id == "aggregate_orders":
            try:
                arguments = json.loads((directory / "payloads" / f"{event.input_refs[0].payload_id}.json").read_text(encoding="utf-8"))
                aggregation_inputs.append((event, arguments["results"]))
            except (OSError, ValueError, KeyError, IndexError):
                errors.append("invalid aggregation inputs")
        if event.event_type not in {"message_sent", "message_delivered", "message_received", "message_consumed"}:
            continue
        try:
            message = Message.model_validate({key: event.details[key] for key in Message.model_fields})
            source = by_id[message.source_event_id]
            if source.status != "completed" or source.agent_id != message.sender_id:
                errors.append("message has invalid producer")
            data = json.loads((directory / "payloads" / f"{message.payload_reference.payload_id}.json").read_text(encoding="utf-8"))
            if digest(data) != message.payload_reference.sha256:
                errors.append("message payload hash mismatch")
            if not any(ref == message.payload_reference for ref in source.output_refs):
                # Sender preserves the actual produced value but may give its payload a new identifier.
                source_values = [json.loads((directory / "payloads" / f"{ref.payload_id}.json").read_text(encoding="utf-8")) for ref in source.output_refs]
                if data not in source_values:
                    errors.append("message payload differs from producer output")
            message_id = message.message_id
            registry = {"message_sent": sent, "message_delivered": delivered,
                        "message_received": received, "message_consumed": consumed}[event.event_type]
            if message_id in registry:
                errors.append(f"duplicate {event.event_type}")
            if event.event_type == "message_sent" and event.agent_id != message.sender_id:
                errors.append("message sender actor mismatch")
            if event.event_type in {"message_received", "message_consumed"} and event.agent_id != message.receiver_id:
                errors.append("message receiver actor mismatch")
            if event.event_type != "message_sent":
                if message_id not in sent or event.details.get("send_event_id") != sent[message_id].event_id:
                    errors.append("message has missing or incorrect send reference")
                elif any(event.details[key] != sent[message_id].details[key] for key in Message.model_fields):
                    errors.append("message envelope changed in transit")
            if event.event_type in {"message_received", "message_consumed"}:
                if message_id not in delivered or event.details.get("delivery_event_id") != delivered[message_id].event_id:
                    errors.append("message has missing or incorrect delivery reference")
            if event.event_type == "message_consumed":
                if message_id not in received or event.details.get("receive_event_id") != received[message_id].event_id:
                    errors.append("message consumed without matching receive")
            link = {"message_sent": (message.source_event_id, "produced_output"),
                    "message_delivered": (event.details.get("send_event_id"), "message_send_delivery"),
                    "message_received": (event.details.get("delivery_event_id"), "message_delivery_receive"),
                    "message_consumed": (event.details.get("delivery_event_id"), "message_delivery_consumption")}[event.event_type]
            if not any((dep.event_id, dep.relationship) == link for dep in event.dependency_refs):
                errors.append("message lifecycle dependency missing")
            registry[message_id] = event
        except (OSError, ValueError, KeyError, IndexError):
            errors.append("invalid message lifecycle evidence")
    for join in joins:
        details = join.details
        required = details.get("required_branches", [])
        accepted = details.get("accepted_result_ids", [])
        if len(accepted) != len(set(accepted)):
            errors.append("join accepted duplicate result")
        branches = []
        for message_id in accepted:
            if message_id not in received or received[message_id].monotonic_ns > join.monotonic_ns:
                errors.append("join accepted an unavailable result")
            else:
                branches.append(received[message_id].details["sender_id"])
        if len(branches) != len(set(branches)) or set(branches) - set(required):
            errors.append("join has unexpected/duplicate branch")
        if set(details.get("missing_branches", [])) != set(required) - set(branches):
            errors.append("join missing-branch evidence inconsistent")
        if details.get("release_reason") == "all_required_inputs" and set(branches) != set(required):
            errors.append("standard join released without all inputs")
        source_ids = {sent[mid].details["source_event_id"] for mid in accepted if mid in sent}
        observed = {dep.event_id for dep in join.dependency_refs if dep.relationship == "accepted_branch_result"}
        if source_ids != observed:
            errors.append("join accepted dependencies inconsistent")
    used = set()
    for event, results in aggregation_inputs:
        accepted_joins = [join for join in joins if any(dep.event_id == join.event_id for dep in event.dependency_refs)]
        if len(accepted_joins) != 1 or set(result.get("message_id") for result in results) != set(accepted_joins[0].details["accepted_result_ids"]):
            errors.append("aggregation results do not match accepted join inputs")
        for result in results:
            try:
                message_id = result["message_id"]
                if message_id in used:
                    errors.append("aggregation used a result twice")
                used.add(message_id)
                consumption = consumed[message_id]
                if consumption.monotonic_ns > event.monotonic_ns or not any(dep.event_id == consumption.event_id for dep in event.dependency_refs):
                    errors.append("aggregation missing consumption dependency")
                if consumption.details["sender_id"] != result["branch_id"]:
                    errors.append("aggregation branch mismatch")
                payload = consumption.details["payload_reference"]["payload_id"]
                facts = json.loads((directory / "payloads" / f"{payload}.json").read_text(encoding="utf-8"))
                if facts != result["facts"]:
                    errors.append("aggregation did not use consumed payload")
            except (OSError, ValueError, KeyError):
                errors.append("aggregation result lacks valid consumption")
    if set(consumed) != used:
        # Failed/cancelled aggregation may have assembled an input without producing an output.
        aggregate_failed = any(e.step_id == "aggregate_orders" and e.status in {"failed", "cancelled", "timed_out"} for e in events)
        if not aggregate_failed:
            errors.append("consumption has no matching aggregation use")
    run_completed = any(e.event_type == "run" and e.status == "completed" for e in events)
    if run_completed and set(sent) != set(delivered):
        errors.append("completed run has undelivered sent messages")
    return errors
