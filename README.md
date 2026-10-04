# ParallelMAS - Stages 2 and 3

A lightweight asynchronous research testbed following section 15 of the supplied guide. Stage 2 provides the runtime, shared model wrapper, recorder and checker. **Stage 3 adds Template A: parallel fact collection and aggregation**, with per-recipient queues, message consumption, all-input joins and an explicit premature-join experiment. Calibration is omitted as requested.

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

The default configuration is now `configs/stage3.yaml`. YAML is validated with Pydantic; unknown settings and inconsistent task, agent count and fault conditions are rejected. Each run saves its fully resolved configuration. Defaults remain one inference slot, eight actions per agent maximum and a 600-second run timeout. Run experiments sequentially.

## Stage 3 workflow

Two or three workers read separate departmental documents using an instrumented local tool. Each asks Qwen3 to extract structured facts and sends its result to one aggregator. The exact document ID is supplied as metadata and must be copied unchanged. Only document metadata/text enter the model prompt; fixture answers and checker totals do not.

The aggregator waits for a join, receives and consumes the accepted messages, then calculates total quantity and cost with deterministic integer arithmetic. The checker requires all document results exactly once, correct worker/document assignments and correct final totals. Extraction is the meaningful model decision; transport, joins, arithmetic and evaluation are deterministic.

Each agent has its own role, context, inbox, semantic step, local sequence, bounded action count and execution status. Agents use instrumented `call_model`, `call_tool`, `send_message`, `receive_message`, `consume_message` and `wait_for_join` wrappers. Only the runtime accesses inboxes. State wrappers remain Stage 4 work.

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

The premature condition explicitly enables `fault.family: premature_join` and sets `workflow.join_release_condition: premature`. Validation prevents silently enabling it for a normal join. The delivery layer holds worker 2's result until the named `aggregate_orders` action finishes, then delivers it without consumption. This completion gate reproduces the selected order without guessing a sleep duration. The configuration records the planned constraint; the actual event evidence shows whether it happened. General scheduling remains Stage 4 work.

The private injection manifest records requested target, target reached, activation, original requirements versus accepted inputs, injection event and outcome. Injection location is not automatically a confirmed root cause. Only the premature-join fault is implemented in this stage.

Configure natural delivery latency through `workflow.message_delays` keyed by worker ID, join timeout through `workflow.join_timeout_seconds` (default 300), and the explicit premature threshold through `workflow.premature_after_results`. Task `aggregation_001` requires three agents; `aggregation_002` requires four. The normal join always waits for all required inputs or records a timeout; it never silently accepts partial results.

## Shared model and execution modes

The asynchronous HTTP wrapper shares one Ollama server and semaphore. Defaults: `qwen3:8b`, 4096 context tokens, 384 output tokens, thinking disabled, temperature 0.2 and seed 42. JSON-schema outputs are validated again with Pydantic. Exact messages, schema/settings, model digest, request hash, raw response, token counts when available, queue duration and request duration are recorded. Malformed/truncated responses are not repaired or regenerated. Reported context errors or a prompt reaching the reported context limit fail explicitly; independent detection of server prompt truncation requires tokenizer instrumentation.

- `scripted_fixture`: known engineering outputs; no network/model calls or real token counts.
- `live`: fresh Qwen3 responses; model digest and Ollama version discovered before inference; no automatic model download.
- `recorded_response`: responses from a previous live run, matched by agent, step and exact request hash including model digest, messages, schema, seed and settings. Mismatches fail explicitly.

On the Qwen3 machine:

```powershell
.venv/Scripts/python.exe -m testbed --config configs/stage3.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage3_premature.yaml --mode live
```

Linux uses `.venv/bin/python`. Full Windows/Linux setup and replay instructions are in [RUN_WITH_QWEN3.md](RUN_WITH_QWEN3.md).

Agents execute asynchronously, with model inference restricted to one shared serving slot. Simultaneous model generation is not claimed. The semaphore constrains this client only; avoid competing requests and concurrent experiments. Seeds do not guarantee identical live outputs. Live behavior must be verified on the target machine.

## Run outputs and interpretation

The console reports `run_directory`, `passed`, mode, runtime status, task correctness and trace validity. Exit code 0 requires completed runtime, correct task output and valid trace. Exit code 1 means at least one check failed. **The premature example intentionally returns `passed: false` and exit code 1, even when its mechanism works correctly.** Inspect the separate outcome fields to distinguish expected task failure from infrastructure or trace errors.

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

Task correctness, contract violations, trace validity, infrastructure errors and model outcomes are separate. Keep private records, resolved experimental settings and replay internals out of future diagnostic inputs. No harmful cause is inferred from connections or nearby timestamps. Resource sampling measures the runtime machine, not a remote model server. Short fixture runs may have one sample.

Validation checks IDs, actor sequences, payload hashes, paired operation events, message producer/envelope/lifecycle consistency, join acceptance and equality between consumed payloads and actual aggregation inputs. Tests cover reference/premature joins, late unconsumed messages, harmless latency, both task sizes, timeouts, tampering, fixture overlap and mock live/replay behavior.

## Stage 2 compatibility and later work

The earlier single-document extraction/checking workflow remains available:

```powershell
.venv/Scripts/python.exe -m testbed --config configs/stage2.yaml
.venv/Scripts/python.exe -m testbed --config configs/stage2.yaml --mode live
```

Its direct controller handoffs do not claim message lifecycle events. The document-ID fix remains in both workflows, with extraction prompt version 1.1. Responses from different prompts cannot replay against changed requests.

Later stages add general scheduling and versioned state, correction and bounded retries, other fault families, SQLite catalogue, dataset exports/labels, inspection UI and pilot collection. Causal attribution remains outside the testbed runtime and recorder.
