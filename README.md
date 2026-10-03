# ParallelMAS — Stage 2

Implements **Stage 2: Runtime and event schema** from section 15 of the supplied research guide. Model calibration is omitted as requested. The default execution uses scripted fixtures and never contacts Ollama. Fixture results are engineering evidence, not live LLM research data.

## Setup and run

Use Python 3.11 or 3.12, with a local virtual environment:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
.venv/Scripts/python.exe -m testbed
.venv/Scripts/python.exe -m unittest discover -s tests -v
```

Run settings are YAML validated with Pydantic; unknown settings are rejected. Each run saves its fully resolved configuration. The default is three agents, eight actions per agent maximum, a 600-second run timeout, and one experiment at a time. Each process runs one experiment; do not launch concurrent live processes against the shared server.

## Implemented workflow

One departmental order document is read through an instrumented local tool. Two asynchronous agents independently extract/check the facts, each with its own role, context, inbox, step, actor-local sequence, action bound, and execution status. A third agent invokes an instrumented deterministic arithmetic tool after both decisions complete and agree. The deterministic outcome checker validates document identity, quantities, integer-cent prices, total cost, and approval.

The controller passes these results directly as tool arguments and records the exact inputs and observed producer dependencies. This Stage 2 workflow is a small foundation workflow, not the full parallel aggregation template. `asyncio.gather` waits for the two tasks; an experimental join service, transport, message delivery/consumption, state versions, fault injection, corrections, and retries belong to later stages. No message consumption events are invented for direct controller handoffs. The agent inbox is reserved for Stage 3; agents do not access it.

Every implemented agent action uses `Runtime.call_tool` or `Runtime.call_model`. Common events include schema/run/event identifiers, one actor identifier, semantic step, local sequence, monotonic time, UTC time, logical operation and attempt identifiers, status, payload references, and typed evidence dependencies. Payload JSON is preserved separately with SHA-256 hashes. File order and timestamp proximity are not causal claims. The recorder does not diagnose failures.

## Shared model and execution modes

The asynchronous HTTP wrapper uses one shared Qwen3 server and semaphore. Defaults: `qwen3:8b`, 4,096 context tokens, 384 generated tokens, `think: false`, temperature 0.2, seed 42, and JSON-schema output validated again by Pydantic. Each request records the assembled messages, schema/settings, exact model digest, request hash, raw response, available token counts, queue duration, and request duration separately. No malformed/truncated response is repaired or regenerated. Reported context errors or a reported prompt reaching the context limit fail explicitly; exact server-side prompt truncation cannot be independently established without server tokenizer instrumentation.

- `scripted_fixture`: default, no network calls and no real token counts or GPU inference measurement.
- `live`: explicitly selected on the model machine. `/api/version` and `/api/tags` establish installed server version and exact model digest. The model must already exist; the runner never downloads a model.
- `recorded_response`: requires a previous live run. Match agent, semantic step, and exact canonical request hash, including schema, model digest, seed, and settings. Mismatches fail instead of reusing a different prompt's response.

On the other machine, configure `model.base_url` as needed, configure the Ollama server with `OLLAMA_MAX_LOADED_MODELS=1` and `OLLAMA_NUM_PARALLEL=1` before starting it, and explicitly run:

```powershell
.venv/Scripts/python.exe -m testbed --mode live
.venv/Scripts/python.exe -m testbed --mode recorded_response --replay-directory runs/run_<live-id>
```

Server configuration and hardware limits must be verified there. The local semaphore limits this client's requests, not other clients. Agents execute asynchronously, with model inference restricted to one shared serving slot. Simultaneous model generation is not claimed. Seeds do not guarantee identical live outputs.

## Output and separation

`runs/run_<id>/` contains `manifest.json`, `config.resolved.yaml`, `task_contract.json`, `events.jsonl`, `payloads/`, `resource_metrics.csv`, `trace_validation.json`, `final_output.json`, `observed_dependencies.json`, and exact-match `model_responses.jsonl` when responses succeed. The manifest includes source commit/dirty status, dependency versions, machine details, prompt version, execution mode, server version/model digest when applicable, and start/end status. Resource sampling records CPU, system RAM, process RSS, and NVIDIA metrics when available; unavailable GPU data is explicit. Short fixture runs may have only one resource sample.

`private/` contains the outcome assessment, an explicitly inactive injection manifest, and unreviewed reference labels with unknown intervention status. Task correctness, contract violations, trace validity, infrastructure errors, and model outcomes are separate. No causal labels are assigned automatically. Do not supply `private/`, resolved experimental configuration, or internal replay files to a future diagnostic method. Dataset export, SQLite catalogue, the inspection UI, full trace semantic checks for future message/state operations, and pilot collection remain later-stage work.

Trace validation currently checks schema, unique IDs, actor sequences, explicit dependencies, payload presence/hashes, and start/terminal pairing. `observed_dependencies.json` is an export of these explicit references, not a diagnostic causal graph. Engineering tests cover the completed workflow, asynchronous operation overlap, model-free fixtures, output validation/truncation with mock HTTP, replay matching, payload tampering, and outcome checking.
