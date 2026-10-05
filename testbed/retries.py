"""Bounded recovery evidence, independent of task correctness and fault attribution."""
import json
from uuid import uuid4
from .schema import DependencyRef


class RetryableToolFailure(Exception):
    pass


def record_component(runtime, kind, details, dependencies):
    actor = "recovery_controller"
    operation, attempt = uuid4().hex, uuid4().hex
    ref = runtime.recorder.payload(details)
    start = runtime.recorder.emit(actor, kind, kind, "started", operation, attempt,
                                  inputs=[ref], dependencies=dependencies)
    return runtime.recorder.emit(actor, kind, kind, "completed", operation, attempt,
        inputs=[ref], outputs=[ref], dependencies=[*dependencies,
        DependencyRef(event_id=start.event_id, relationship="operation_start")], **details)


def retry_decision(runtime, terminal, reason, retry):
    return record_component(runtime, "retry_decision", {
        "logical_operation_id": terminal.operation_id, "previous_attempt_id": terminal.attempt_id,
        "previous_attempt_event_id": terminal.event_id,
        "attempt_number": terminal.details["attempt_number"],
        "failure_reason": reason, "retry": retry, "max_additional_retries": runtime.config.workflow.max_additional_retries},
        [DependencyRef(event_id=terminal.event_id, relationship="failed_attempt_retry")])


def accept_result(runtime, terminal, accepted, reason=None):
    return record_component(runtime, "operation_result", {
        "logical_operation_id": terminal.operation_id, "final_attempt_id": terminal.attempt_id,
        "final_attempt_event_id": terminal.event_id, "accepted": accepted,
        "failure_reason": reason, "recovered": accepted and terminal.details["attempt_number"] > 1},
        [DependencyRef(event_id=terminal.event_id, relationship="accepted_attempt")])


async def bounded_tool(runtime, agent, arguments, dependencies):
    logical_id = uuid4().hex
    previous = None
    for number in range(1, runtime.config.workflow.max_additional_retries + 2):
        async def invoke(evidence):
            if arguments["approval_consumed"]:
                if (arguments["plan"]["previous_version"] != arguments["initial_plan"]["version"]
                        or not arguments["plan"]["approved"]):
                    raise ValueError("approved_plan_does_not_reference_initial_version")
            elif arguments["plan"] != arguments["initial_plan"]:
                raise ValueError("unapproved_action_changed_initial_plan")
            if runtime.fault_controller.fail_tool(number, arguments, evidence["_terminal_event_id"]):
                raise RetryableToolFailure("mock_transient_tool_failure")
            return {"executed": True, "plan": arguments["plan"]}
        try:
            value, terminal, _ = await runtime.action(agent, "execute_delivery", "tool_call", arguments,
                invoke, dependencies, {"tool_name": "mock_delivery", "tool_version": "1.0"},
                operation_id=logical_id, attempt_number=number,
                previous_attempt=previous.attempt_id if previous else None)
        except RetryableToolFailure:
            terminal = runtime.recorder.events[-1]
            retry = number <= runtime.config.workflow.max_additional_retries
            decision = retry_decision(runtime, terminal, "mock_transient_tool_failure", retry)
            if not retry:
                accept_result(runtime, terminal, False, "mock_transient_tool_failure")
                return {"executed": False, "plan": arguments["plan"], "recovered": False,
                        "accepted_attempt_event_id": None, "failure_reason": "mock_transient_tool_failure"}
            previous = terminal
            dependencies = [*dependencies,
                DependencyRef(event_id=terminal.event_id, relationship="failed_attempt_retry"),
                DependencyRef(event_id=decision.event_id, relationship="retry_decision")]
        else:
            accept_result(runtime, terminal, True)
            return {**value, "recovered": number > 1, "accepted_attempt_event_id": terminal.event_id,
                    "failure_reason": None}


def validate_retry_evidence(directory, events):
    errors, groups = [], {}
    by_id = {e.event_id: e for e in events}
    for event in events:
        if event.status != "started" and event.event_type in {"tool_call", "state_write"}:
            groups.setdefault(event.operation_id, []).append(event)
    def linked(e, source, relation):
        return any(d.event_id == source and d.relationship == relation for d in e.dependency_refs)
    for operation, attempts in groups.items():
        for index, event in enumerate(attempts):
            if event.details.get("attempt_number", 1) != index + 1 or index >= 2:
                errors.append("invalid or unbounded retry attempt numbering")
            if index:
                previous = attempts[index - 1]
                decisions = [by_id[d.event_id] for d in event.dependency_refs if d.relationship == "retry_decision"]
                if (event.details.get("previous_attempt") != previous.attempt_id
                        or not linked(event, previous.event_id, "failed_attempt_retry")
                        or len(decisions) != 1 or not decisions[0].details.get("retry")
                        or decisions[0].details.get("previous_attempt_event_id") != previous.event_id):
                    errors.append("retry lacks preceding attempt and decision")
            elif event.details.get("previous_attempt") is not None:
                errors.append("first attempt has a preceding attempt")
        results = [e for e in events if e.event_type == "operation_result" and e.status == "completed"
                   and e.details.get("logical_operation_id") == operation]
        if results and (len(results) != 1 or results[0].details.get("final_attempt_event_id") != attempts[-1].event_id):
            errors.append("workflow accepted a nonfinal or duplicate attempt")
        for first, second in zip(attempts, attempts[1:]):
            if first.agent_id != second.agent_id or first.event_type != second.event_type or first.attempt_id == second.attempt_id:
                errors.append("retry changed actor/type or reused attempt identifier")
    for event in events:
        if event.status != "completed" or event.event_type not in {"retry_decision", "operation_result"}:
            continue
        try:
            detail = event.details
            previous = by_id[detail["previous_attempt_event_id"] if event.event_type == "retry_decision" else detail["final_attempt_event_id"]]
            if detail["logical_operation_id"] != previous.operation_id:
                errors.append("recovery logical operation mismatch")
            if event.event_type == "retry_decision":
                reason = previous.details.get("rejection_reason") or previous.details.get("error")
                if (previous.status == "completed" and previous.details.get("accepted") is not False
                        or detail["failure_reason"] != reason
                        or detail["previous_attempt_id"] != previous.attempt_id
                        or not linked(event, previous.event_id, "failed_attempt_retry")
                        or detail["retry"] != (previous.details["attempt_number"] <= detail["max_additional_retries"])):
                    errors.append("invalid retry decision evidence")
            elif (detail["final_attempt_id"] != previous.attempt_id
                    or not linked(event, previous.event_id, "accepted_attempt")
                    or detail["accepted"] != (previous.status == "completed" and previous.details.get("accepted", True))):
                errors.append("invalid final accepted attempt")
            value = json.loads((directory / "payloads" / f"{event.output_refs[0].payload_id}.json").read_text(encoding="utf-8"))
            if value != detail:
                errors.append("recovery payload/details mismatch")
        except (KeyError, IndexError, OSError, ValueError):
            errors.append("invalid recovery evidence")
    return errors
