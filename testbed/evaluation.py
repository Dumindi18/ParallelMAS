"""Deterministic task evaluation, separate from runtime and evidence collection."""


def check_outcome(task, output):
    if output is None:
        return {"task_correctness": "unknown", "execution_contract_violations": []}
    expected = task["requirements"]
    correct = output["document_id"] == task["document_id"] and all(output.get(k) == v for k, v in expected.items())
    violations = [] if output.get("approved") else ["required_independent_approval_missing"]
    return {"task_correctness": "success" if correct and not violations else "failure",
            "execution_contract_violations": violations}
