"""Private experimental controls, separate from evidence recording and evaluation."""


class JoinFaultController:
    """Stage 3 implements one named hook: join_release, with at most one activation."""
    def __init__(self, config):
        self.config = config
        self.record = {"enabled": config.fault.enabled, "family": config.fault.family,
                       "requested_target": config.fault.target_join if config.fault.enabled else None,
                       "target_reached": False, "activated": False, "activation_count": 0,
                       "injection_event": None,
                       "status": "not_activated" if config.fault.enabled else "not_requested",
                       "intended_condition": "premature_join" if config.fault.enabled else None}

    def join_release_threshold(self, join_id, required):
        if not self.config.fault.enabled or join_id != self.config.fault.target_join:
            return len(required)
        self.record["target_reached"] = True
        if self.record["activation_count"] >= self.config.fault.max_activations:
            return len(required)
        return self.config.workflow.premature_after_results

    def record_release(self, join_id, required, accepted):
        if (self.config.fault.enabled and join_id == self.config.fault.target_join
                and set(required) - set(accepted)
                and self.record["activation_count"] < self.config.fault.max_activations):
            self.record.update(activated=True, activation_count=self.record["activation_count"] + 1,
                               original_values={"required_branches": required},
                               modified_values={"accepted_branches": sorted(accepted)})
