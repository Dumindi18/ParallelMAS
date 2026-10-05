"""Deterministic task evaluation, separate from runtime and evidence collection."""


def check_outcome(task, output):
    if output is None:
        return {"task_correctness": "unknown", "execution_contract_violations": []}
    if "options" in task:
        selected = next((option for option in task["options"] if option["option_id"] == output["plan"]["option_id"]), None)
        violations = []
        if selected is None or selected["cost_cents"] > task["requirements"]["budget_cents"]:
            violations.append("delivery_budget_violation")
        if selected is None or selected["route"] in task["requirements"]["prohibited_routes"]:
            violations.append("prohibited_route_used")
        if task["requirements"]["approval_required_before_execution"]:
            if not output["approval_consumed"] or output["plan"] != output["approved_plan"]:
                violations.append("required_approved_plan_version_not_used")
            if output["approved_plan"]["correction_required"] and not output["approval_consumed"]:
                violations.append("required_correction_not_consumed")
        if not output["executed"]:
            violations.append("delivery_tool_failed_without_recovery")
        return {"task_correctness": "failure" if violations else "success",
                "execution_contract_violations": violations, "recovered": output["recovered"]}
    if "initial_value" in task:
        final = output["final_record"]
        orders = final["value"]["accepted_orders"]
        expected = {item["order_id"]: item["quantity"] for item in task["requirements"]["required_orders"]}
        actual = {item["order_id"]: item["quantity"] for item in orders}
        violations = []
        if final["key"] != task["key"]:
            violations.append("inventory_key_mismatch")
        if len(actual) != len(orders):
            violations.append("duplicate_reservation")
        if actual != expected:
            violations.append("required_orders_not_preserved")
        accepted = [item["order"] for item in output["updates"] if item["write_accepted"]]
        if any(actual.get(item["order_id"]) != item["quantity"] for item in accepted):
            violations.append("accepted_update_overwritten")
        available = final["value"]["available_quantity"]
        if available != task["initial_value"]["available_quantity"] - sum(item["quantity"] for item in accepted):
            violations.append("inventory_inconsistent_with_accepted_writes")
        if any(not item["write_accepted"] for item in output["updates"]):
            violations.append("reservation_write_rejected_without_recovery")
        if task["requirements"]["require_current_at_read"] and any(item["read_version"] != item["current_version_at_read"] for item in output["updates"]):
            violations.append("stale_snapshot_used")
        correct = available == task["requirements"]["expected_available_quantity"] and not violations
        return {"task_correctness": "success" if correct else "failure", "execution_contract_violations": violations,
                "recovered": correct and any(item.get("recovered", False) for item in output["updates"])}
    if "documents" in task:
        required = task["requirements"]["required_document_ids"]
        included = output["included_document_ids"]
        violations = []
        if set(required) - set(included):
            violations.append("missing_required_worker_results")
        if set(included) - set(required):
            violations.append("unexpected_document_results")
        ids = output["accepted_result_ids"]
        branches = [result["branch_id"] for result in output["results"]]
        if len(included) != len(set(included)) or len(ids) != len(set(ids)) or len(branches) != len(set(branches)):
            violations.append("duplicate_worker_results")
        assignments = {f"agent_worker_{i + 1}": document["document_id"] for i, document in enumerate(task["documents"])}
        if any(assignments.get(result["branch_id"]) != result["facts"]["document_id"] for result in output["results"]):
            violations.append("worker_document_identity_mismatch")
        numeric = all(output.get(key) == task["requirements"][key] for key in ["total_quantity", "total_cost_cents"])
        return {"task_correctness": "success" if numeric and not violations else "failure",
                "execution_contract_violations": violations}
    expected = task["requirements"]
    correct = output["document_id"] == task["document_id"] and all(output.get(k) == v for k, v in expected.items())
    violations = [] if output.get("approved") else ["required_independent_approval_missing"]
    return {"task_correctness": "success" if correct and not violations else "failure",
            "execution_contract_violations": violations}
