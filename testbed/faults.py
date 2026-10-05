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


class StateFaultController:
    """Stage 4 named hooks: state_read and state_write; one fault per run."""
    def __init__(self, config):
        self.config = config
        self.record = {"enabled": config.fault.enabled, "family": config.fault.family,
                       "requested_target": {"key": config.fault.target_key, "agent_id": config.fault.target_agent} if config.fault.enabled else None,
                       "target_reached": False, "activated": False, "activation_count": 0,
                       "injection_event": None, "status": "not_activated" if config.fault.enabled else "not_requested",
                       "intended_condition": config.fault.family if config.fault.enabled else None}

    def matches(self, agent_id, key, family):
        return (self.config.fault.enabled and self.config.fault.family == family
                and agent_id == self.config.fault.target_agent and key == self.config.fault.target_key
                and self.record["activation_count"] < self.config.fault.max_activations)

    def read_version(self, agent_id, key):
        if self.matches(agent_id, key, "stale_state"):
            self.record["target_reached"] = True
            return self.config.fault.stale_version
        return None

    def record_read(self, agent_id, key, result, event_id):
        if self.matches(agent_id, key, "stale_state") and result["record"]["version"] < result["current_record"]["version"]:
            self.record.update(activated=True, activation_count=1, injection_event=event_id,
                               original_values=result["current_record"], modified_values=result["record"])

    def record_write(self, agent_id, key, result, event_id):
        if self.matches(agent_id, key, "lost_update"):
            self.record["target_reached"] = True
            if result["accepted"] and result["base_version"] < result["previous_current_version"]:
                self.record.update(activated=True, activation_count=1, injection_event=event_id,
                                   original_values={"version": result["previous_current_version"], "value": result["previous_value"]},
                                   modified_values={"version": result["new_version"], "value": result["new_value"]})
