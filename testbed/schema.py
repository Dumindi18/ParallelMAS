from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Experiment(StrictModel):
    name: str
    task_id: Literal["orders_001", "aggregation_001", "aggregation_002", "inventory_001", "inventory_002", "delivery_001", "delivery_002"]
    mode: Literal["live", "recorded_response", "scripted_fixture"] = "scripted_fixture"
    seed: int = 42
    replay_directory: str | None = None


class RuntimeConfig(StrictModel):
    agents: Literal[3, 4] = 3
    max_steps_per_agent: int = Field(default=8, ge=1, le=8)
    run_timeout_seconds: float = Field(default=600, gt=0)


class ModelConfig(StrictModel):
    base_url: str = "http://localhost:11434"
    name: Literal["qwen3:8b"] = "qwen3:8b"
    inference_slots: Literal[1] = 1
    num_ctx: int = Field(default=4096, ge=1)
    num_predict: int = Field(default=384, ge=1)
    think: Literal[False] = False
    temperature: float = Field(default=0.2, ge=0, le=2)


class RecordingConfig(StrictModel):
    capture_model_inputs: Literal[True] = True
    capture_payloads: Literal[True] = True
    resource_sample_interval_seconds: float = Field(default=1, gt=0)


class WorkflowConfig(StrictModel):
    template: Literal["stage2_facts", "parallel_aggregation", "shared_record", "plan_correction"] = "stage2_facts"
    join_release_condition: Literal["all_inputs", "premature"] = "all_inputs"
    premature_after_results: int = Field(default=1, ge=1)
    join_timeout_seconds: float = Field(default=300, gt=0)
    message_delays: dict[str, float] = Field(default_factory=dict)
    hold_branch_until_aggregation: str | None = None
    state_write_policy: Literal["unconditional", "compare_and_set"] = "compare_and_set"
    max_additional_retries: Literal[0, 1] = 0
    correction_policy: Literal["wait", "delivered_unconsumed", "after_execution"] = "wait"
    correction_delay_seconds: float = Field(default=0, ge=0)
    mock_tool_fail_once: bool = False

    @model_validator(mode="after")
    def nonnegative_delays(self):
        if any(delay < 0 for delay in self.message_delays.values()):
            raise ValueError("message delays must be nonnegative")
        return self


class FaultConfig(StrictModel):
    enabled: bool = False
    family: Literal["premature_join", "lost_update", "stale_state", "delayed_correction", "unconsumed_correction", "tool_failure"] | None = None
    target_join: Literal["department_orders"] = "department_orders"
    max_activations: Literal[1] = 1
    target_key: Literal["inventory.item_A"] = "inventory.item_A"
    target_agent: Literal["agent_updater_B"] = "agent_updater_B"
    stale_version: Literal[0] = 0


class ScheduleConfig(StrictModel):
    mode: Literal["natural", "controlled"] = "natural"
    profile: Literal["natural", "a_then_b", "b_then_a", "both_read_before_writes"] = "natural"
    gate_timeout_seconds: float = Field(default=300, gt=0)

    @model_validator(mode="after")
    def consistent_mode(self):
        if (self.mode == "natural") != (self.profile == "natural"):
            raise ValueError("natural mode requires natural profile; controlled mode requires a named profile")
        return self


class Config(StrictModel):
    experiment: Experiment
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    model: ModelConfig = Field(default_factory=ModelConfig)
    recording: RecordingConfig = Field(default_factory=RecordingConfig)
    workflow: WorkflowConfig = Field(default_factory=WorkflowConfig)
    fault: FaultConfig = Field(default_factory=FaultConfig)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)

    @model_validator(mode="after")
    def replay_requires_source(self):
        if self.experiment.mode == "recorded_response" and not self.experiment.replay_directory:
            raise ValueError("recorded_response requires replay_directory")
        parallel = self.workflow.template == "parallel_aggregation"
        shared = self.workflow.template == "shared_record"
        correction = self.workflow.template == "plan_correction"
        if correction != self.experiment.task_id.startswith("delivery_"):
            raise ValueError("delivery task_id requires plan_correction template")
        if parallel != self.experiment.task_id.startswith("aggregation_"):
            raise ValueError("task_id and workflow template must match")
        if shared != self.experiment.task_id.startswith("inventory_"):
            raise ValueError("inventory task_id requires shared_record template")
        required_agents = 4 if self.experiment.task_id == "aggregation_002" else 3
        if self.runtime.agents != required_agents:
            raise ValueError(f"this task requires {required_agents} agents")
        premature = self.workflow.join_release_condition == "premature"
        if not shared and not correction and (premature != self.fault.enabled or (self.fault.enabled and self.fault.family != "premature_join")):
            raise ValueError("premature release requires an explicit enabled premature_join fault")
        if not parallel and (premature or self.workflow.message_delays or self.workflow.hold_branch_until_aggregation):
            raise ValueError("message/join conditions require parallel_aggregation")
        branches = {f"agent_worker_{i+1}" for i in range(required_agents - 1)}
        if set(self.workflow.message_delays) - branches:
            raise ValueError("message delay references an unknown worker")
        held = self.workflow.hold_branch_until_aggregation
        if held and (not premature or held not in branches):
            raise ValueError("held branch must be a known worker in a premature experiment")
        if premature and self.workflow.premature_after_results >= len(branches):
            raise ValueError("premature threshold must be below all required branches")
        if held and self.workflow.premature_after_results > len(branches) - 1:
            raise ValueError("premature threshold must be reachable before releasing the held branch")
        if parallel and self.runtime.max_steps_per_agent < 2 * len(branches) + 2:
            raise ValueError("parallel aggregator needs 2 * workers + 2 actions")
        if not shared and self.schedule.mode != "natural":
            raise ValueError("named state scheduling profiles require shared_record")
        if shared:
            if self.runtime.max_steps_per_agent < 4:
                raise ValueError("shared-record updaters require four actions")
            if self.fault.enabled and self.fault.family not in {"lost_update", "stale_state"}:
                raise ValueError("shared_record supports only lost_update and stale_state faults")
            if self.fault.enabled and self.schedule.mode != "controlled":
                raise ValueError("state fault demonstrations require a controlled schedule")
            if self.fault.enabled and self.fault.family == "lost_update":
                if self.schedule.profile != "both_read_before_writes" or self.workflow.state_write_policy != "unconditional":
                    raise ValueError("lost_update requires both_read_before_writes and unconditional writes")
            if self.fault.enabled and self.fault.family == "stale_state" and self.schedule.profile != "a_then_b":
                raise ValueError("stale_state requires a_then_b so an older version exists at the target read")
        if self.workflow.max_additional_retries and not correction:
            if not shared or self.workflow.state_write_policy != "compare_and_set" or self.schedule.profile not in {"both_read_before_writes", "natural"}:
                raise ValueError("state retries require compare_and_set with conflicting or natural execution")
            if self.runtime.max_steps_per_agent < 7:
                raise ValueError("state recovery requires seven actions")
        if not correction and (self.workflow.correction_policy != "wait" or self.workflow.correction_delay_seconds or self.workflow.mock_tool_fail_once):
            raise ValueError("correction/tool conditions require plan_correction")
        if correction:
            policy = self.workflow.correction_policy
            family = {"after_execution": "delayed_correction", "delivered_unconsumed": "unconsumed_correction"}.get(policy)
            if self.workflow.mock_tool_fail_once:
                if family:
                    raise ValueError("one fault per run: tool failure cannot accompany a correction fault")
                family = "tool_failure"
            if self.fault.enabled != bool(family) or self.fault.family != family:
                raise ValueError("correction/tool fault must be explicitly enabled with matching family")
            if self.runtime.max_steps_per_agent < 6:
                raise ValueError("correction executor requires six actions including recovery")
        return self


class PayloadRef(StrictModel):
    payload_id: str
    sha256: str


class DependencyRef(StrictModel):
    event_id: str
    relationship: Literal["actor_local_order", "operation_start", "produced_output",
                          "message_send_delivery", "message_delivery_receive",
                          "message_delivery_consumption", "accepted_branch_result",
                          "state_write_read", "state_read_write", "state_previous_write",
                          "schedule_order", "checkpoint_action", "failed_attempt_retry", "retry_decision", "accepted_attempt"]


class Event(StrictModel):
    schema_version: Literal["1.0"] = "1.0"
    run_id: str
    event_id: str
    agent_id: str | None = None
    component_id: str | None = None
    step_id: str
    event_type: str
    local_sequence: int = Field(ge=1)
    monotonic_ns: int
    wall_time_utc: str
    operation_id: str
    attempt_id: str
    status: Literal["started", "completed", "failed", "timed_out", "cancelled"]
    input_refs: list[PayloadRef] = Field(default_factory=list)
    output_refs: list[PayloadRef] = Field(default_factory=list)
    dependency_refs: list[DependencyRef] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def one_actor(self):
        if (self.agent_id is None) == (self.component_id is None):
            raise ValueError("exactly one actor identifier is required")
        return self


class Facts(StrictModel):
    document_id: str
    quantity: int = Field(ge=0, strict=True)
    unit_cost_cents: int = Field(ge=0, strict=True)


class OrderDecision(StrictModel):
    order_id: str
    quantity: int = Field(ge=1, strict=True)


class DeliveryPlan(StrictModel):
    option_id: str
    version: Literal[1]


class Message(StrictModel):
    message_id: str
    sender_id: str
    receiver_id: str
    message_type: Literal["worker_result", "initial_plan", "approval", "correction"] = "worker_result"
    payload_reference: PayloadRef
    source_event_id: str
