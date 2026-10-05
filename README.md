# ParallelMAS - Stages 2 through 5

A lightweight asynchronous research testbed following section 15 of the supplied guide. Stage 2 provides the runtime, shared model wrapper, recorder and checker. Stage 3 adds Template A parallel aggregation. Stage 4 adds Template C versioned shared-record updates and named scheduling gates. **Stage 5 adds Template B plan checking, correction and dependent execution, plus bounded tool and inventory-conflict recovery.** Calibration is omitted as requested.

The default is a model-free **scripted fixture**. Fixtures and mock HTTP tests are engineering evidence, not live Qwen3 research data. The development machine does not need a model.

## Setup

Use Python 3.11 or 3.12. From the project folder on Windows:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
.venv/Scripts/python.exe -m testbed
.venv/Scripts/python.exe -m unittest discover -s tests -v
```

On Linux:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m testbed
.venv/bin/python -m unittest discover -s tests -v
```

The default configuration is now `configs/stage5.yaml`. YAML is validated with Pydantic; unknown settings and inconsistent task, schedule, write policy, agent count and fault conditions are rejected. Each run saves its fully resolved configuration. Defaults remain one inference slot, eight actions per agent maximum and a 600-second run timeout. Run experiments sequentially.

## Stage 5 correction and retries

Template B runs three agents asynchronously: a planner selects a structured delivery option, a checker examines the budget and prohibited-route constraints, and an executor performs a bounded local mock delivery. The checker explicitly approves version 1 or sends a corrected version 2. The executor's normal policy waits for and consumes that approval/correction before execution. The task contract states whether prior approval is required; a compliant early action is permitted only in the separate optional-approval task.

The planner uses the shared Qwen wrapper in live mode. The checker and delivery tool use deterministic local code. All initial plans, approvals and corrections pass through the existing per-recipient queues and instrumented send/delivery/receive/consumption wrappers. Tool inputs preserve the selected plan, the initial version it references and whether approval was consumed. Delivery alone never becomes a consumption edge.

```powershell
.venv/Scripts/python.exe -m testbed
.venv/Scripts/python.exe -m testbed --config configs/stage5_tool_recovery.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage5_conflict_recovery.yaml
```

Use `.venv/bin/python` on Linux. These commands use fixtures and require no Ollama installation.

| Configuration | Scripted fixture result |
|---|---|
| `stage5.yaml` | Invalid initial plan corrected to allowed standard delivery, version 2 consumed; success |
| `stage5_delayed.yaml` | Named gate holds correction until delivery action finishes; invalid initial version executed; task failure |
| `stage5_unconsumed.yaml` | Correction delivered and received before action, left unused; task failure |
| `stage5_harmless_delay.yaml` | Transport latency, executor still waits and consumes correction; success |
| `stage5_tool_failure.yaml` | Mock delivery fails once, retries disabled; task failure |
| `stage5_tool_recovery.yaml` | Same failure, one additional attempt succeeds; recovered success |
| `stage5_conflict_recovery.yaml` | Both inventory updaters read version 0; B conflicts, refreshes and retries; both reservations retained, quantity 3; recovered success |
| `stage5_optional_approval.yaml` | A compliant initial plan may execute before approval under its explicit contract; success; no correction fault activation |

The fixture for `delivery_001` intentionally selects the invalid express option to exercise correction. It is excluded from the public task contract and live prompts. Fresh Qwen responses can already be compliant, in which case the checker sends an approval and the requested correction fault may not activate. Live runs are not forced to reproduce the fixture decision. Inspect actual messages and the private activation record rather than assuming that a configuration name proves activation. Missing required approval still violates `delivery_001` even when the selected option is compliant.

`workflow.max_additional_retries` accepts only 0 or 1. Selected mock delivery attempts and reservation writes retain one logical `operation_id`, with distinct `attempt_id` values, attempt numbers and previous-attempt references. Instrumented `retry_decision` records preserve the failure/conflict reason and whether another attempt is allowed. `operation_result` identifies the final attempt accepted or rejected by the workflow; dependency exports include failed-attempt-to-retry and accepted-attempt edges. Recovery is recorded separately from task failure and infrastructure errors.

Inventory recovery rereads current state and rebuilds the proposal from that fresh snapshot. It retains the already validated order quantity rather than regenerating model output. No lock spans read-modify-write. The recovering updater uses seven actions, within the eight-action limit. Mock tool recovery retries only the explicit transient tool error; malformed model output, replay mismatches and infrastructure failures are never silently retried. A rejected write creates no version. A second failed attempt ends recovery.

Correction faults and the fail-once tool hook live in the separate fault controller. Validation rejects implicit correction faults, combined tool/correction faults, more than one additional retry, and insufficient action budgets. Controlled late correction uses a completion gate rather than a guessed sleep. The harmless latency example uses an ordinary configurable transport delay. Private records distinguish activation, task failure, recovered success, harmless activation and unknown outcomes; injection location is not a causal diagnosis.

The delivery output separates `correction_recovered` from `tool_recovered`; overall `recovered` is true when either recovery leads to successful execution. Stage 5 validation checks that consumed plans appear in actual dependent tool inputs, retry links identify the preceding attempt and decision, and the final delivery output matches the accepted attempt. Tests include persistent tool failure, fresh-state conflict recovery, exact mocked live/replay matching, cancellation after model failure and evidence tampering. No real model is called by these local tests.

## Stage 4 scheduling and shared state

Two updating agents process separate inventory orders and one verifier inspects the final record. Qwen3 interprets each order document into structured `order_id` and `quantity` fields; identifiers are supplied explicitly and copied unchanged. The model input includes the actual snapshot key, version and value. Deterministic code prepares the reservation update, writes the state and checks outcomes. Expected answers, injection controls and fixture responses are never included in live model inputs.

Each run starts with **one shared record**, `inventory.item_A`, reset to version 0. Every version preserves `key`, `version`, `value` and its exact `writer_event_id`. State operations pass through instrumented `read_state` and `write_state` wrappers. Reads record the returned version/value, originating write and actual current version. Writes record base/expected version, previous current version/value, previous writer, acceptance/rejection and new version/value.

- **Unconditional writes** accept a proposal based on an older snapshot and can overwrite another agent's reservation.
- **Compare-and-set** accepts only if the expected version matches the current version; conflicts are recorded without changing state or creating a new version.

Each accepted write creates a version even if its value is unchanged. Each individual state operation is atomic; no lock covers the entire read-modify-write sequence. Snapshot copies prevent agents from mutating stored history outside the wrappers.

### Named scheduling profiles

`schedule.mode: natural` with `profile: natural` adds no ordering constraints. `mode: controlled` uses named gates around the instrumented read/write actions:

| Profile | Required action order |
| --- | --- |
| `a_then_b` | A reads, A writes, B reads, B writes |
| `b_then_a` | B reads, B writes, A reads, A writes |
| `both_read_before_writes` | A reads, B reads, A writes, B writes |

Gates wait on completed checkpoints using asynchronous events, not guessed sleep durations. Actual state actions and released checkpoints are recorded. Planned constraints live in `private/schedule_plan.json`; actual checkpoint evidence lives in `observed_schedule.json`. `private/schedule_assessment.json` checks whether the planned order actually occurred. A gate timeout or interrupted schedule is recorded rather than claimed as reproduced. These initial named profiles apply to the shared-record template; Stage 3's delivery completion gate remains available separately.

### Stage 4 experiments

```powershell
.venv/Scripts/python.exe -m testbed --config configs/stage4.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage4_reverse.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage4_lost_update.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage4_stale_state.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage4_conflict.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage4_natural.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage4_second_task.yaml
```

Linux uses `.venv/bin/python` instead. All commands above use fixtures unless `--mode live` is supplied.

| Configuration | Expected fixture behavior |
| --- | --- |
| `stage4.yaml` | Serialized CAS updates; both orders retained; final available quantity 3; success |
| `stage4_reverse.yaml` | Same reservations in reverse order; quantity 3; success |
| `stage4_lost_update.yaml` | Both agents read version 0; unconditional B write overwrites A; quantity 6; task failure |
| `stage4_stale_state.yaml` | After A writes version 1, B receives version 0; stale snapshot overwrites A; quantity 6; task failure |
| `stage4_conflict.yaml` | Both read version 0; CAS rejects B after A writes; quantity 7; task failure without recovery |
| `stage4_natural.yaml` | Unconstrained asynchronous CAS; actual outcome depends on order/conflicts |
| `stage4_second_task.yaml` | Second inventory task, initial quantity 20 and orders of 5 and 6; final quantity 9; success |

The task contract requires both valid orders to remain in state, correct quantities, inventory consistent with accepted writes and a current snapshot at the instant of each updater read. Merely becoming older later while another branch executes is not automatically a read-freshness violation. The deterministic checker reports missing/overwritten reservations, unrecovered rejection and stale-snapshot use separately from trace validity.

`lost_update` explicitly requires an enabled fault, `both_read_before_writes` and unconditional writes. `stale_state` uses a separate named state-read hook, supplying historical version 0 to updater B after A's write. Private records include target reached, activation, original/selected values and the exact injection event. Only one fault activates per run. The lost-update condition consists of schedule plus write policy, with no additional payload mutation. Both histories and the actual read/write dependencies remain observable. Injection activation is not a confirmed causal label.

The Stage 4 configurations retain `workflow.max_additional_retries: 0`. Their conflict example demonstrates CAS protection from overwrite but fails because one required reservation is missing. Stage 5's separate `stage5_conflict_recovery.yaml` enables one additional attempt.

## Stage 3 workflow

Two or three workers read separate departmental documents using an instrumented local tool. Each asks Qwen3 to extract structured facts and sends its result to one aggregator. The exact document ID is supplied as metadata and must be copied unchanged. Only document metadata/text enter the model prompt; fixture answers and checker totals do not.

The aggregator waits for a join, receives and consumes the accepted messages, then calculates total quantity and cost with deterministic integer arithmetic. The checker requires all document results exactly once, correct worker/document assignments and correct final totals. Extraction is the meaningful model decision; transport, joins, arithmetic and evaluation are deterministic.

Each agent has its own role, context, inbox, semantic step, local sequence, bounded action count and execution status. Agents use instrumented `call_model`, `call_tool`, `send_message`, `receive_message`, `consume_message` and `wait_for_join` wrappers. Only the runtime accesses inboxes. Stage 4 adds instrumented state read/write wrappers for its separate shared-record workflow.

### Message and join evidence

Messages include a unique ID, sender, receiver, type, payload reference/hash and producer event. Four separate actions are recorded:

1. `message_sent`: result handed to transport.
2. `message_delivered`: result available in the recipient's inbox.
3. `message_received`: removed from the inbox; this alone does not imply use.
4. `message_consumed`: supplied to the aggregation step.

Consumption is connected to the actual aggregation tool input. Late deliveries receive no invented consumption edge, and duplicate results cannot be consumed twice. The delivery layer supports per-worker delays.

Joins record their identifier, required branches, completed worker computations, available delivered branches, accepted result IDs, missing branches, release reason and timeout status. Computation completion, delivery and acceptance are distinct. Required branches do not automatically get observed consumption dependencies. The observed export includes typed send/delivery/receive/consumption links, producer-to-join acceptance and actual input dependencies.

### Experiment configurations

```powershell
.venv/Scripts/python.exe -m testbed --config configs/stage3.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage3_delayed.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage3_three_workers.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage3_premature.yaml
```

For Linux, replace the executable with `.venv/bin/python`.

| Configuration | Condition | Expected fixture result |
| --- | --- | --- |
| `stage3.yaml` | Two workers, ordinary all-input join | Quantity 10, cost 1475 cents, success |
| `stage3_delayed.yaml` | Worker 2 delivery delayed by 0.1 seconds; join waits | Same successful result |
| `stage3_three_workers.yaml` | Three workers, four agents, second task instance | Quantity 12, cost 2900 cents, success |
| `stage3_premature.yaml` | Explicit join release after one result | Completed runtime, valid trace, failed task |

The premature condition explicitly enables `fault.family: premature_join` and sets `workflow.join_release_condition: premature`. Validation prevents silently enabling it for a normal join. The delivery layer holds worker 2's result until the named `aggregate_orders` action finishes, then delivers it without consumption. This completion gate reproduces the selected order without guessing a sleep duration. The configuration records the planned constraint; the actual event evidence shows whether it happened. Stage 4 supplies named read/write schedules for the shared-record template.

The private injection manifest records requested target, target reached, activation, original requirements versus accepted inputs, injection event and outcome. Injection location is not automatically a confirmed root cause. Stage 3 implements premature joins; Stage 4 additionally implements stale-state and lost-update conditions.

Configure natural delivery latency through `workflow.message_delays` keyed by worker ID, join timeout through `workflow.join_timeout_seconds` (default 300), and the explicit premature threshold through `workflow.premature_after_results`. Task `aggregation_001` requires three agents; `aggregation_002` requires four. The normal join always waits for all required inputs or records a timeout; it never silently accepts partial results.

## Shared model and execution modes

The asynchronous HTTP wrapper shares one Ollama server and semaphore. Defaults: `qwen3:8b`, 4096 context tokens, 384 output tokens, thinking disabled, temperature 0.2 and seed 42. JSON-schema outputs are validated again with Pydantic. Exact messages, schema/settings, model digest, request hash, raw response, token counts when available, queue duration and request duration are recorded. Malformed/truncated responses are not repaired or regenerated. Reported context errors or a prompt reaching the reported context limit fail explicitly; independent detection of server prompt truncation requires tokenizer instrumentation.

- `scripted_fixture`: known engineering outputs; no network/model calls or real token counts.
- `live`: fresh Qwen3 responses; model digest and Ollama version discovered before inference; no automatic model download.
- `recorded_response`: responses from a previous live run, matched by agent, step and exact request hash including model digest, messages, schema, seed and settings. Mismatches fail explicitly.

On the Qwen3 machine:

```powershell
.venv/Scripts/python.exe -m testbed --config configs/stage4.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage4_lost_update.yaml --mode live
```

Linux uses `.venv/bin/python`. Full Windows/Linux setup and replay instructions are in [RUN_WITH_QWEN3.md](RUN_WITH_QWEN3.md).

Agents execute asynchronously, with model inference restricted to one shared serving slot. Simultaneous model generation is not claimed. The semaphore constrains this client only; avoid competing requests and concurrent experiments. Seeds do not guarantee identical live outputs. Live behavior must be verified on the target machine.

## Run outputs and interpretation

The console reports `run_directory`, `passed`, mode, runtime status, task correctness and trace validity. Exit code 0 requires completed runtime, correct task output and valid trace. Exit code 1 means at least one check failed. **The premature-join, lost-update, stale-state and unrecovered-conflict examples intentionally return `passed: false` and exit code 1, even when their mechanisms work correctly.** Inspect the separate outcome fields to distinguish expected task failure from infrastructure or trace errors.

Each `runs/run_<id>/` contains:

| File/folder | Purpose |
| --- | --- |
| `manifest.json` | Stage/mode, source commit/dirty status, dependencies, machine, prompt/server/model versions, start/end status |
| `config.resolved.yaml` | Actual experimental settings |
| `task_contract.json` | Task specification without scripted answer fixtures |
| `events.jsonl` | Versioned actions, statuses, local sequences, monotonic/UTC timestamps and references |
| `payloads/` | Actual data and model inputs/responses preserved with SHA-256 hashes |
| `resource_metrics.csv` | CPU, system RAM, process RSS and available NVIDIA metrics |
| `final_output.json` | Included documents/results and totals |
| `trace_validation.json` | Structural and message/join/consumption integrity checks |
| `observed_dependencies.json` | Explicit observed links; no causal diagnosis |
| `model_responses.jsonl` | Successful responses for exact-match replay |
| `private/` | Injection manifest, outcome assessment and unreviewed labels |

Stage 4 additionally saves `state_history.json` (all accepted versions including overwritten values), `observed_schedule.json` (actual checkpoints), and private schedule plan/assessment files. The final output includes the verifier's final record, per-updater read versions and accepted/rejected write results. The observed dependency export includes originating-write-to-read, read-to-write, previous-write and actual scheduling edges; it does not identify harmful causes.

Task correctness, contract violations, trace validity, infrastructure errors and model outcomes are separate. Keep private records, resolved experimental settings and replay internals out of future diagnostic inputs. No harmful cause is inferred from connections or nearby timestamps. Resource sampling measures the runtime machine, not a remote model server. Short fixture runs may have one sample.

Validation checks IDs, actor sequences, payload hashes, paired operation events, message producer/envelope/lifecycle consistency, join acceptance and equality between consumed payloads and actual aggregation inputs. Stage 4 validates read/write provenance, CAS acceptance, version progression, history exports and actual checkpoints. A faithfully recorded race or stale read can have a valid trace while its task fails. Tests cover reference/premature joins, late unconsumed messages, shared-state races, stale reads, CAS conflicts, named-order reproduction, timeouts, tampering and mock live/replay behavior.

## Earlier-stage compatibility and later work

The earlier single-document extraction/checking workflow remains available:

```powershell
.venv/Scripts/python.exe -m testbed --config configs/stage2.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage2.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage3.yaml --mode live
```

Its direct controller handoffs do not claim message lifecycle events. The document-ID fix remains in both workflows, with extraction prompt version 1.1. Responses from different prompts cannot replay against changed requests.

Stages 2 through 5 are implemented, including all three core workflow templates and bounded tool/conflict recovery. Corrupted worker results and their detection/correction remain unimplemented Template A conditions. Later work adds those remaining fault conditions, SQLite catalogue, dataset exports/labels, inspection UI and Stage 6 pilot collection. Causal attribution remains outside the testbed runtime and recorder.
