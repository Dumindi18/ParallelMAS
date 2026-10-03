# Run Stage 2 on the machine with Qwen3:8b

No Python code changes are required. The system already supports Qwen3 through Ollama. These instructions assume the target machine runs Windows and already has `qwen3:8b` installed in Ollama.

## 1. Copy the project

Copy this project folder to the target machine, including `testbed/`, `tasks/`, `configs/`, and `requirements.txt`. Do not copy `.venv`; create a new environment on that machine. Existing `runs/` are optional and are not needed for a new live run.

Open PowerShell in the copied project folder. Use Python 3.11 or 3.12:

```powershell
python --version
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
```

## 2. Configure and start Ollama

To follow the guide's initial single-model, single-inference settings:

1. Quit Ollama from its system-tray menu if it is running.
2. Open Windows **Edit environment variables for your account**.
3. Add or update these user environment variables:

   | Variable | Value |
   | --- | --- |
   | `OLLAMA_MAX_LOADED_MODELS` | `1` |
   | `OLLAMA_NUM_PARALLEL` | `1` |

4. Start Ollama again from the Start menu.
5. Open a new PowerShell window in the project folder and check:

```powershell
ollama --version
ollama list
Invoke-RestMethod http://localhost:11434/api/version
```

`ollama list` must include the exact model name `qwen3:8b`. The runner does not download models. If you use a terminal-managed Ollama server instead of the desktop application, stop the existing server and start it in a separate PowerShell window with:

```powershell
$env:OLLAMA_MAX_LOADED_MODELS = '1'
$env:OLLAMA_NUM_PARALLEL = '1'
ollama serve
```

Keep that window open while running the testbed from another window. Use one server-start method; do not start a second server on the same port.

Official setup reference: [Ollama FAQ](https://docs.ollama.com/faq).

## 3. Check the testbed configuration

`configs/stage2.yaml` already contains the required defaults:

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

Leave these settings as they are when Ollama runs locally on its default port. Change only `model.base_url` if your server uses a different address or port. Keep the rest of the configuration file intact.

The default `experiment.mode` is `scripted_fixture`. The command below overrides it for a live run without editing the file. To make live execution the default, change `experiment.mode` to `live`.

## 4. Run the workflow using Qwen3

From the project folder:

```powershell
.venv/Scripts/python.exe -m testbed --mode live
```

The system discovers the Ollama version and exact model digest, then runs the three-agent workflow. Two agents independently extract facts from the departmental order document. The third uses a deterministic tool to calculate the total when their results agree.

Agents execute asynchronously, with model inference restricted to one shared serving slot. Run one experiment at a time and avoid other requests to the model during research runs. Seeds do not guarantee identical live responses.

The program prints the new run directory and a `passed` flag. Exit code `0` means the workflow completed, its answer was correct, and its trace passed validation. Exit code `1` means at least one of those checks failed; inspect the saved records.

## 5. Inspect the saved results

Each execution writes to `runs/run_<unique-id>/`:

| File or folder | Contents |
| --- | --- |
| `manifest.json` | Execution mode, model digest, server version, machine/dependency details, and run status |
| `config.resolved.yaml` | Actual configuration used, including the live-mode override |
| `task_contract.json` | Task description and correctness requirements |
| `events.jsonl` | Actions, timestamps, statuses, payload references, and explicit dependencies |
| `payloads/` | Actual tool/model inputs and outputs, including captured responses |
| `final_output.json` | Final workflow result |
| `trace_validation.json` | Trace integrity checks |
| `resource_metrics.csv` | CPU, RAM, process memory, and available NVIDIA metrics |
| `observed_dependencies.json` | Explicit recorded event relationships |
| `model_responses.jsonl` | Successful captured responses for exact-match replay |
| `private/outcome_assessment.json` | Task correctness, contract violations, model outcomes, and infrastructure failures |

The supplied task's expected quantity is `7`, unit price is `125` cents, and total is `875` cents. A successful run also requires independent approval.

Private records remain separate from evidence for a future diagnostic method. Live behavior must be verified on the target machine; local fixture tests do not establish live model performance.

## 6. Optional engineering checks and replay

Run the tests or a fixture workflow without calling a model:

```powershell
.venv/Scripts/python.exe -m unittest discover -s tests -v
.venv/Scripts/python.exe -m testbed --mode scripted_fixture
```

To replay responses from a previous live run, replace `run_<live-id>` with its actual directory name:

```powershell
.venv/Scripts/python.exe -m testbed --mode recorded_response --replay-directory runs/run_<live-id>
```

Replay requires a live source run and exact matching of agent, step, model digest, assembled request, and settings. A changed prompt or setting produces an explicit mismatch instead of silently reusing another response. A fixture run cannot be used as the source for recorded-response mode.

## Troubleshooting

- **Connection refused:** Ensure Ollama is running and `model.base_url` matches its address. Check the `/api/version` command above.
- **Model not found:** Confirm `ollama list` includes `qwen3:8b` with that exact name.
- **Malformed output, truncated response, or thinking output:** Inspect `events.jsonl` and captured payloads. The runner records these failures and does not silently repair or regenerate the answer. Verify that the installed Ollama/model supports structured outputs and `think: false`.
- **Timeout:** The default run timeout is 600 seconds. Inspect timings and machine resources before changing `runtime.run_timeout_seconds` in the YAML configuration.
- **Missing GPU metrics:** NVIDIA measurements require an available `nvidia-smi`; unavailable GPU data is recorded explicitly. CPU and RAM measurements are still collected.

This guide runs Stage 2 only. Calibration, message transport, experimental joins, shared-state races, controlled faults, corrections, retries, and pilot collection remain outside this implementation stage.
