# Run ParallelMAS Stages 2 through 4 with Qwen3:8b

No Python source changes are needed. The project defaults to model-free fixtures; select `--mode live` on the machine that already has `qwen3:8b`. The default workflow is now **Stage 5 correction and bounded retries**. Stages 2, 3 and 4 remain selectable.

## 1. Copy/update and install

Copy the updated `testbed/`, `tasks/`, `configs/`, `tests/`, `requirements.txt` and documentation. Include `correction.py`, `retries.py`, the updated runtime/schema/model/transport/evaluation/validation/fault modules, both delivery tasks and all Stage 5 configurations. Create a virtual environment on the target machine rather than copying `.venv`. Preserve previous runs if their evidence is needed. No new dependency is required beyond `requirements.txt`.

Use Python 3.11 or 3.12. From the project folder on Windows:

```powershell
python --version
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
.venv/Scripts/python.exe -m unittest discover -s tests -v
```

On Linux, including the machine used for the supplied live manifest:

```bash
python3 --version
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
```

Tests use fixtures/mock HTTP and do not call the real model.

## 2. Configure Ollama

Check the installed server and exact model name:

```text
ollama --version
ollama list
```

`ollama list` must include `qwen3:8b`. Configure these server variables before restarting Ollama:

| Variable | Value |
| --- | --- |
| `OLLAMA_MAX_LOADED_MODELS` | `1` |
| `OLLAMA_NUM_PARALLEL` | `1` |

On Windows with the desktop application, quit Ollama from its tray menu, add the variables in **Edit environment variables for your account**, then start Ollama again from the Start menu.

For a terminal-managed Windows server, stop the existing server and run in a separate PowerShell window:

```powershell
$env:OLLAMA_MAX_LOADED_MODELS = '1'
$env:OLLAMA_NUM_PARALLEL = '1'
ollama serve
```

For a terminal-managed Linux server, stop the existing server and run in a separate terminal:

```bash
OLLAMA_MAX_LOADED_MODELS=1 OLLAMA_NUM_PARALLEL=1 ollama serve
```

For an existing Linux systemd service, use a service override instead of launching a second server:

```bash
sudo systemctl edit ollama.service
```

Add and save:

```ini
[Service]
Environment="OLLAMA_MAX_LOADED_MODELS=1"
Environment="OLLAMA_NUM_PARALLEL=1"
```

Then restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart ollama.service
```

Use the method appropriate to your installation, and avoid two servers on port 11434. Keep a terminal-managed server window open. Official reference: [Ollama FAQ](https://docs.ollama.com/faq).

Check connectivity on Windows:

```powershell
Invoke-RestMethod http://localhost:11434/api/version
```

On Linux:

```bash
curl http://localhost:11434/api/version
```

## 3. Check model settings

All experiment files use these settings, explicitly or through validated defaults:

```yaml
model:
  base_url: http://localhost:11434
  name: qwen3:8b
  inference_slots: 1
  num_ctx: 4096
  num_predict: 384
  think: false
  temperature: 0.2
```

Leave them unchanged for local Ollama on the default port. Change `model.base_url` in the chosen configuration only if the address differs. The runner records the exact installed digest/version and never downloads a model. Verify the installed model/server supports structured outputs and thinking disabled.

The live prompts contain document text and exact document/order identifiers. Stage 4 also supplies the actual inventory snapshot key, version and value. Models copy the supplied identifiers; they do not guess them. Fixture answers, expected totals and private fault controls are not supplied to the model.

## Run Stage 5 correction and bounded retries

After the Ollama setup above, run these sequentially on the target machine. Every command explicitly selects fresh live model responses.

Linux:

```bash
.venv/bin/python -m testbed --config configs/stage5.yaml --mode live
.venv/bin/python -m testbed --config configs/stage5_delayed.yaml --mode live
.venv/bin/python -m testbed --config configs/stage5_unconsumed.yaml --mode live
.venv/bin/python -m testbed --config configs/stage5_harmless_delay.yaml --mode live
.venv/bin/python -m testbed --config configs/stage5_tool_failure.yaml --mode live
.venv/bin/python -m testbed --config configs/stage5_tool_recovery.yaml --mode live
.venv/bin/python -m testbed --config configs/stage5_conflict_recovery.yaml --mode live
.venv/bin/python -m testbed --config configs/stage5_optional_approval.yaml --mode live
```

Windows:

```powershell
.venv/Scripts/python.exe -m testbed --config configs/stage5.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage5_delayed.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage5_unconsumed.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage5_harmless_delay.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage5_tool_failure.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage5_tool_recovery.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage5_conflict_recovery.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage5_optional_approval.yaml --mode live
```

Template B uses Qwen only to select the initial structured delivery plan. The checker enforces budget and prohibited-route constraints using local deterministic code and explicitly approves a version, correcting an invalid plan when needed. The executor invokes a local mock delivery tool; it sends no real delivery request. The normal executor waits for and consumes the checked plan before acting. The model input contains task instructions, options and public constraints, with no fixture answer or private injection settings.

| Configuration | Expected behavior when model output is valid |
|---|---|
| `stage5.yaml` | Checked version consumed before action; success |
| `stage5_delayed.yaml` | Checked message held until after execution; required approval unused; task failure |
| `stage5_unconsumed.yaml` | Checked message delivered/received before execution but unused; required approval unused; task failure |
| `stage5_harmless_delay.yaml` | Delayed checked message still consumed before execution; success |
| `stage5_tool_failure.yaml` | One mock transient failure; no retry; task failure |
| `stage5_tool_recovery.yaml` | One mock transient failure, one retry, accepted second result; recovered success |
| `stage5_conflict_recovery.yaml` | CAS conflict, fresh state read, rebuilt proposal, one retry; both correct orders retained; quantity 3 |
| `stage5_optional_approval.yaml` | Contract permits compliant early execution; success if the initial plan is compliant |

The scripted `delivery_001` fixture intentionally begins with invalid express delivery to demonstrate correction. A live Qwen run may select a compliant option immediately. Then the checker sends approval rather than correction, and a requested correction fault may remain `not_activated`. The code does not replace a live model answer to force activation. Check `private/injection_manifest.json` and the actual message type before treating a run as evidence of an activated correction fault. Missing mandatory approval remains a task failure even if the delivery option itself is compliant. The optional-approval task demonstrates that late checking is not inherently an error.

`max_additional_retries` is limited to 0 or 1. Tool attempts share a logical operation ID and have distinct attempt IDs. The first mock error remains a failed event even when the task recovers. Inventory retry reads current state and rebuilds the proposal from the validated order decision; it does not regenerate that decision or hold a lock across the sequence. The recovering updater uses seven actions. Malformed/truncated model output and replay mismatches are recorded without model regeneration.

For each Stage 5 run, inspect `events.jsonl`, `final_output.json`, `trace_validation.json`, `observed_dependencies.json` and `private/outcome_assessment.json`. Look for `retry_decision` and `operation_result` events, their previous/final attempt IDs, failure reason and final accepted result. `final_output.json` records the selected and approved plan versions, approval consumption, accepted delivery attempt and recovery. Inventory runs additionally preserve `state_history.json` and actual/private schedule evidence as described below. An activated tool failure followed by successful recovery is labelled `activated_recovered`; labels remain separate from causal attribution.

Intentional failure configurations return `passed: false` and exit code 1, while `status: completed` and `trace_validity: true` can still hold. Check task correctness and infrastructure failures separately. Live Qwen errors or wrong inventory quantities can change the expected task outcome; these examples are not substitutes for calibration and live measurements on the target machine.

Local model-free verification and exact response replay:

```bash
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m testbed --config configs/stage5_tool_recovery.yaml --mode scripted_fixture
.venv/bin/python -m testbed --config configs/stage5.yaml --mode recorded_response --replay-directory runs/run_<matching-live-id>
.venv/bin/python -m testbed --config configs/stage5_conflict_recovery.yaml --mode recorded_response --replay-directory runs/run_<matching-inventory-live-id>
```

Replace the example source directory with an actual live run. Use the source task, model settings and schedule; changed model inputs must match a recorded request exactly. On Windows replace `.venv/bin/python` with `.venv/Scripts/python.exe`. The tests use fixtures and mock HTTP responses and never call a real model.

## 4. Run Stage 4 scheduling and shared state

Two updating agents interpret separate order documents, prepare deterministic inventory reservations, and write one versioned shared record. A third agent reads the final record for deterministic evaluation. Every run resets that record to version 0. The guide's starting one-record scope is preserved.

On Linux, run selected experiments sequentially:

```bash
.venv/bin/python -m testbed --config configs/stage4.yaml --mode live
.venv/bin/python -m testbed --config configs/stage4_reverse.yaml --mode live
.venv/bin/python -m testbed --config configs/stage4_lost_update.yaml --mode live
.venv/bin/python -m testbed --config configs/stage4_stale_state.yaml --mode live
.venv/bin/python -m testbed --config configs/stage4_conflict.yaml --mode live
.venv/bin/python -m testbed --config configs/stage4_natural.yaml --mode live
.venv/bin/python -m testbed --config configs/stage4_second_task.yaml --mode live
```

On Windows, use `.venv/Scripts/python.exe` instead of `.venv/bin/python`, for example:

```powershell
.venv/Scripts/python.exe -m testbed --config configs/stage4.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage4_lost_update.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage4_stale_state.yaml --mode live
```

| Configuration | Expected result when order interpretation is correct |
| --- | --- |
| `stage4.yaml` | A completes before B reads; compare-and-set preserves both orders; quantity 3; success |
| `stage4_reverse.yaml` | B completes before A reads; same quantity 3; success |
| `stage4_lost_update.yaml` | A and B read version 0; A then B write unconditionally; quantity 6; expected task failure |
| `stage4_stale_state.yaml` | After A writes, B receives version 0 although version 1 is current; quantity 6; expected task failure |
| `stage4_conflict.yaml` | Both read version 0; compare-and-set rejects B after A writes; quantity 7; expected task failure without retry |
| `stage4_natural.yaml` | No named ordering constraints; actual outcome depends on operation order/conflicts |
| `stage4_second_task.yaml` | Initial quantity 20, orders of 5 and 6, serialized compare-and-set; quantity 9; success |

Controlled profiles use named asynchronous gates around reads/writes, not sleep-based race timing. Gate timeout defaults to 300 seconds; the overall run timeout remains 600. A state lock covers each individual operation, not the full read-modify-write sequence. Rejected compare-and-set writes leave the current version unchanged. Accepted writes always create a new version, even for an unchanged value; all previous versions are preserved.

The task requires both valid reservations, correct quantities, inventory consistent with accepted writes and a current snapshot at the instant of each updater read. The stale experiment specifically supplies an older snapshot at that instant. The lost-update experiment uses valid reads followed by an overwrite due to schedule and unconditional write policy; no extra data mutation is added.

The Stage 4 files retain `workflow.max_additional_retries: 0`. Stage 5 adds a separate conflict-recovery configuration with one additional attempt. Do not treat the unrecovered-conflict run as an infrastructure error merely because its task fails.

**For the lost-update, stale-state and conflict examples, `passed: false` and exit code 1 are expected**, alongside completed runtime and a valid trace. The first two should also have private records confirming the corresponding fault activation. The conflict example has no injected fault and demonstrates version-check protection without recovery. Live model mistakes may produce other outcomes; inspect the actual evidence.

## 5. Stage 3 compatibility

On Linux, run these sequentially:

```bash
.venv/bin/python -m testbed --config configs/stage3.yaml --mode live
.venv/bin/python -m testbed --config configs/stage3_delayed.yaml --mode live
.venv/bin/python -m testbed --config configs/stage3_three_workers.yaml --mode live
.venv/bin/python -m testbed --config configs/stage3_premature.yaml --mode live
```

On Windows:

```powershell
.venv/Scripts/python.exe -m testbed --config configs/stage3.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage3_delayed.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage3_three_workers.yaml --mode live
.venv/Scripts/python.exe -m testbed --config configs/stage3_premature.yaml --mode live
```

Do not chain experiments with `&&` if later commands must run after an intentional task failure. Each invocation creates a separate run folder.

| Configuration | Expected behavior |
| --- | --- |
| `stage3.yaml` | Two workers and one aggregator; all results required; quantity 10 and cost 1475 cents |
| `stage3_delayed.yaml` | Same task with delayed worker-2 delivery; join still waits for both results |
| `stage3_three_workers.yaml` | Three workers and one aggregator; quantity 12 and cost 2900 cents |
| `stage3_premature.yaml` | Explicit premature release after one result; expected task failure |

In the premature experiment, worker 2's message is held until the named aggregation action finishes, then delivered without consumption. Its join fault must be explicitly enabled; ordinary joins always wait for all inputs or record a timeout. Worker facts are model-generated; aggregation arithmetic is deterministic.

Agents execute asynchronously, with model inference restricted to one shared serving slot. Run one experiment at a time and avoid competing client requests. Seeds do not guarantee identical live outputs. `python -m testbed` now defaults to `configs/stage5.yaml`, still in fixture mode unless overridden. To default a selected file to live execution, change `experiment.mode` to `live` in that YAML file.

## 6. Interpret the console result

The console includes `run_directory`, `passed`, execution mode, `status`, `task_correctness` and `trace_validity`.

- `passed: true`, exit code 0: completed workflow, correct output and valid trace.
- `passed: false`, exit code 1: one or more checks failed; evidence is preserved.
- For the premature example, **completed runtime, failed task and valid trace are expected**. Check the private injection manifest for actual activation. The `passed` flag evaluates the task, not whether a failure experiment was successfully demonstrated.

The same interpretation applies to the Stage 4 controlled failure examples. A requested schedule alone is not proof that it occurred; check the actual checkpoints and private schedule assessment.

Normal, delayed and three-worker live runs should succeed when Qwen3 extracts the facts correctly. Live model mistakes can still fail a task; fixture success alone does not establish live success.

## 7. Inspect the run folder

Each `runs/run_<id>/` contains:

| File/folder | Contents |
| --- | --- |
| `manifest.json` | Stage/mode, model digest, server version, machine/dependencies, status and correctness |
| `config.resolved.yaml` | Actual configuration, including live override |
| `task_contract.json` | Required documents and correct totals, without fixture answers |
| `events.jsonl` | Actions, timings, payload references and typed dependencies |
| `payloads/` | Actual messages, facts, requests, responses and tool inputs/outputs |
| `final_output.json` | Included document/result IDs, facts and totals |
| `trace_validation.json` | Structural and message/join/consumption checks |
| `observed_dependencies.json` | Explicit observed links, without causal attribution |
| `resource_metrics.csv` | Runtime-machine CPU, RAM, process RSS and available NVIDIA metrics |
| `model_responses.jsonl` | Successful captured responses for replay |
| `private/injection_manifest.json` | Target, activation, injection event and experimental outcome |
| `private/outcome_assessment.json` | Correctness, contract violations, model/infrastructure errors |
| `private/reference_labels.json` | Unreviewed labels; no confirmed causal attribution |

For Stage 4, also inspect:

| File | Contents |
| --- | --- |
| `state_history.json` | Initial record and all accepted versions, values and writer event IDs; rejected writes remain in the trace |
| `observed_schedule.json` | Actual completed read/write checkpoints and corresponding state events |
| `private/schedule_plan.json` | Requested mode/profile and ordered constraints |
| `private/schedule_assessment.json` | Actual versus planned order, satisfaction and missing checkpoints |

Stage 4 `final_output.json` contains the verifier's final record and both updater outcomes. Read events identify the version actually returned and its originating write, plus the current version at read time. Write events show base/expected version, previous current version, new version/value, acceptance and conflict reason. These facts allow reconstruction of stale reads and overwritten updates without assigning a causal label.

Joins distinguish required branches, completed computations, delivered/available branches and accepted inputs. In the premature example, worker 2's delivery must follow the completed `aggregate_orders` action, with no consumption event for that result. Actual event evidence establishes the order; configuration only specifies the intended order.

Keep private records and resolved experimental conditions out of future diagnostic inputs. An injected event is not automatically a confirmed root cause.

## 8. Fixtures, replay and Stage 2

Model-free checks on Linux:

```bash
.venv/bin/python -m testbed --config configs/stage3.yaml --mode scripted_fixture
.venv/bin/python -m testbed --config configs/stage3_premature.yaml --mode scripted_fixture
.venv/bin/python -m testbed --config configs/stage4.yaml --mode scripted_fixture
.venv/bin/python -m testbed --config configs/stage4_lost_update.yaml --mode scripted_fixture
.venv/bin/python -m testbed --config configs/stage4_stale_state.yaml --mode scripted_fixture
```

Windows uses `.venv/Scripts/python.exe`. Premature-join, lost-update and stale-state commands intentionally exit with code 1.

Replay a previous live run using the corresponding configuration and actual directory name:

```bash
.venv/bin/python -m testbed --config configs/stage3.yaml --mode recorded_response --replay-directory runs/run_<live-id>
.venv/bin/python -m testbed --config configs/stage4.yaml --mode recorded_response --replay-directory runs/run_<inventory-live-id>
```

Replay requires a live source and exact matching of agent, step, model digest, request and settings. Changed prompts, seeds or settings cause explicit mismatches. In Stage 4 a changed schedule may change a snapshot and therefore the actual model input; the old response must not be silently reused. Use the matching task and original schedule for baseline replay. Random recorder event IDs are kept out of model prompts, while their evidence dependencies remain recorded. Fixtures cannot be replay sources. Deterministic aggregation/state arithmetic does not require model responses.

The original Stage 2 workflow remains available:

```bash
.venv/bin/python -m testbed --config configs/stage2.yaml --mode live
```

It independently extracts/checks one document and requires approval; its expected total is 875 cents.

## Troubleshooting

- **Connection refused:** Start Ollama, check `/api/version` and `model.base_url`.
- **Model missing:** Confirm `qwen3:8b` appears exactly in `ollama list`.
- **Normal run fails correctness:** Inspect facts in `final_output.json` and `model_responses.jsonl`, then the private outcome assessment. Runtime completion is separate from task correctness.
- **Premature run fails correctness:** Expected if the trace is valid and private records confirm activation; inspect these fields before treating it as infrastructure failure.
- **Malformed/truncated/thinking output:** Inspect saved model events/responses; the runner does not repair or regenerate answers silently.
- **Join timeout:** Inspect completed/delivered/accepted branches. Join timeout defaults to 300 seconds and overall timeout to 600. Review timings/resources before changing them.
- **Schedule gate timeout:** Inspect the failing gate and missing checkpoints. A preceding model/action may not have completed. The saved assessment must not be interpreted as successful reproduction if constraints are incomplete.
- **Stage 4 conflict:** A rejected write creates no new version and is not retried in that configuration. Use `stage5_conflict_recovery.yaml` to refresh state and retry once.
- **Stage 4 stale/lost-update failure:** Inspect complete history, actual read versions, current versions, overwritten values and private activation records. A valid trace can faithfully record a failed task.
- **Replay mismatch:** Use the correct task and unchanged request settings; old prompts cannot match changed requests.
- **Missing GPU metrics:** Requires available `nvidia-smi`; CPU/RAM still work. Local monitoring does not measure a remote server's GPU.

Stage 5 correction and bounded retries are implemented. The core workflows A, B and C are available. Corrupted-result conditions for Template A remain later work, along with Stage 6 pilot collection and inspection tooling. Causal attribution remains outside this testbed.
