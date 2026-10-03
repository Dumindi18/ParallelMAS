from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Experiment(StrictModel):
    name: str
    task_id: Literal["orders_001"]
    mode: Literal["live", "recorded_response", "scripted_fixture"] = "scripted_fixture"
    seed: int = 42
    replay_directory: str | None = None


class RuntimeConfig(StrictModel):
    agents: Literal[3] = 3
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


class Config(StrictModel):
    experiment: Experiment
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    model: ModelConfig = Field(default_factory=ModelConfig)
    recording: RecordingConfig = Field(default_factory=RecordingConfig)

    @model_validator(mode="after")
    def replay_requires_source(self):
        if self.experiment.mode == "recorded_response" and not self.experiment.replay_directory:
            raise ValueError("recorded_response requires replay_directory")
        return self


class PayloadRef(StrictModel):
    payload_id: str
    sha256: str


class DependencyRef(StrictModel):
    event_id: str
    relationship: Literal["actor_local_order", "operation_start", "produced_output"]


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
