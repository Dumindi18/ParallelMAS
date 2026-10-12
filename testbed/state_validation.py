"""Validate recorded state/schedule consistency without treating races as malformed traces."""
import json


def validate_state_evidence(directory, events):
    relevant = [event for event in events if event.event_type in {"state_initialized", "state_read", "state_write"}]
    if not relevant:
        return []
    errors, history, current = [], {}, {}
    by_id = {event.event_id: event for event in events}

    def payload(ref):
        return json.loads((directory / "payloads" / f"{ref.payload_id}.json").read_text(encoding="utf-8"))

    def linked(event, source, relationship):
        return any(dep.event_id == source and dep.relationship == relationship for dep in event.dependency_refs)

    for event in relevant:
        if event.status != "completed":
            continue
        try:
            detail = event.details
            key = detail["key"]
            output = payload(event.output_refs[0])
            if event.event_type == "state_initialized":
                expected = {field: detail[field] for field in ["key", "version", "value", "writer_event_id"]}
                if key in history or expected["version"] != 0 or expected["writer_event_id"] != event.event_id or output != expected:
                    errors.append("invalid initial state provenance")
                history[key] = [expected]
                current[key] = expected
                continue
            previous = current[key]
            if event.event_type == "state_read":
                selected = history[key][detail["returned_version"]]
                if detail["returned_version"] < 0 or selected["value"] != detail["returned_value"]:
                    errors.append("read value differs from recorded version")
                if detail["originating_write_event_id"] != selected["writer_event_id"] or not linked(event, selected["writer_event_id"], "state_write_read"):
                    errors.append("read missing originating write dependency")
                if detail["current_version"] != previous["version"] or detail["current_writer_event_id"] != previous["writer_event_id"]:
                    errors.append("read current-version evidence inconsistent")
                if output != {"record": selected, "current_record": previous}:
                    errors.append("read payload differs from state history")
                # A correctly recorded old read is valid evidence, even if it violates the task contract.
                continue
            proposal = payload(event.input_refs[0])
            if detail["policy"] not in {"unconditional", "compare_and_set"}:
                errors.append("unknown recorded state policy")
            should_accept = detail["policy"] == "unconditional" or detail["expected_version"] == previous["version"]
            if detail["accepted"] != should_accept:
                errors.append("write acceptance contradicts policy/version")
            if (detail["previous_current_version"] != previous["version"] or detail["previous_value"] != previous["value"]
                    or detail["previous_writer_event_id"] != previous["writer_event_id"]
                    or not linked(event, previous["writer_event_id"], "state_previous_write")):
                errors.append("write previous-state evidence inconsistent")
            reads = [by_id[dep.event_id] for dep in event.dependency_refs if dep.relationship == "state_read_write"]
            if (len(reads) != 1 or reads[0].event_type != "state_read" or reads[0].status != "completed"
                    or reads[0].agent_id != event.agent_id or reads[0].details["returned_version"] != detail["base_version"]):
                errors.append("write base version lacks matching read")
            if detail["expected_version"] != detail["base_version"] or proposal["base_version"] != detail["base_version"]:
                errors.append("write proposal/base-version mismatch")
            proposals = [by_id[dep.event_id] for dep in event.dependency_refs if dep.relationship == "produced_output"]
            if not any(source.status == "completed" and payload(source.output_refs[0]) == proposal for source in proposals):
                errors.append("write differs from prepared proposal")
            fields = ["key", "policy", "base_version", "expected_version", "previous_current_version", "previous_value",
                      "previous_writer_event_id", "accepted", "rejection_reason", "new_version", "new_value", "writer_event_id"]
            if output != {field: detail[field] for field in fields}:
                errors.append("write result payload/details mismatch")
            if detail["accepted"]:
                if detail["new_version"] != previous["version"] + 1 or detail["writer_event_id"] != event.event_id or detail["new_value"] != proposal["value"]:
                    errors.append("accepted write has invalid new-version provenance")
                record = {"key": key, "version": detail["new_version"], "value": detail["new_value"], "writer_event_id": event.event_id}
                history[key].append(record)
                current[key] = record
            elif detail["new_version"] is not None or detail["new_value"] is not None or detail["writer_event_id"] is not None or detail["rejection_reason"] != "version_conflict":
                errors.append("rejected write created a version or lacks conflict reason")
        except (OSError, ValueError, KeyError, IndexError, TypeError):
            errors.append("invalid state operation evidence")

    observed = []
    state_action_ids = {event.event_id for event in relevant if event.status == "completed" and event.event_type != "state_initialized"}
    checkpoint_actions = set()
    for event in events:
        if event.status != "completed":
            continue
        try:
            if event.event_type == "schedule_gate":
                for dep in event.dependency_refs:
                    if dep.relationship == "schedule_order":
                        predecessor = by_id[dep.event_id]
                        if predecessor.event_type != "schedule_checkpoint" or predecessor.status != "completed" or predecessor.monotonic_ns > event.monotonic_ns:
                            errors.append("gate released before predecessor completed")
            if event.event_type == "schedule_checkpoint":
                action = by_id[event.details["action_event_id"]]
                expected = f"{action.agent_id}.{action.step_id}"
                if action.event_type not in {"state_read", "state_write"} or action.status != "completed" or expected != event.details["checkpoint"]:
                    errors.append("checkpoint does not match state action")
                if not linked(event, action.event_id, "checkpoint_action") or action.monotonic_ns > event.monotonic_ns:
                    errors.append("checkpoint missing action dependency")
                gates = [by_id[dep.event_id] for dep in action.dependency_refs if dep.relationship == "schedule_order"]
                if len(gates) != 1 or gates[0].event_type != "schedule_gate" or gates[0].details["checkpoint"] != expected or gates[0].monotonic_ns > action.monotonic_ns:
                    errors.append("state action has no preceding matching gate")
                if action.event_id in checkpoint_actions:
                    errors.append("duplicate state checkpoint completion")
                checkpoint_actions.add(action.event_id)
                observed.append({"checkpoint": expected, "event_id": event.event_id,
                                 "action_event_id": action.event_id, "monotonic_ns": event.monotonic_ns})
        except (KeyError, TypeError):
            errors.append("invalid schedule checkpoint evidence")
    if state_action_ids != checkpoint_actions:
        errors.append("state actions missing checkpoint completion")
    try:
        exported = json.loads((directory / "state_history.json").read_text(encoding="utf-8"))
        expected_records = [record for records in history.values() for record in records]
        if exported["records"] != expected_records:
            errors.append("exported state history differs from trace")
        schedule = json.loads((directory / "observed_schedule.json").read_text(encoding="utf-8"))
        if schedule["checkpoints"] != observed:
            errors.append("exported observed schedule differs from trace")
        final = json.loads((directory / "final_output.json").read_text(encoding="utf-8"))
        if final is not None:
            if final["final_record"] != current[final["final_record"]["key"]]:
                errors.append("final inventory differs from latest state version")
            for update in final["updates"]:
                write = by_id[update["write_event_id"]]
                if write.event_type != "state_write" or write.agent_id != update["agent_id"] or write.details["accepted"] != update["write_accepted"]:
                    errors.append("final update differs from write evidence")
                read = next(by_id[dep.event_id] for dep in write.dependency_refs if dep.relationship == "state_read_write")
                if (update["read_version"] != read.details["returned_version"]
                        or update["current_version_at_read"] != read.details["current_version"]
                        or update.get("attempts", 1) != write.details.get("attempt_number", 1)
                        or update.get("recovered", False) != (write.details["accepted"] and write.details.get("attempt_number", 1) > 1)):
                    errors.append("final update read/recovery differs from accepted write")
    except (OSError, ValueError, KeyError, TypeError, StopIteration):
        errors.append("missing/invalid state or schedule export")
    return errors
