"""Deterministic task evaluation, separate from runtime and evidence collection."""


def check_outcome(task, output):
    if output is None:
        return {"task_correctness": "unknown", "execution_contract_violations": []}
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
