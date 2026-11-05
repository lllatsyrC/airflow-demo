# Starting jobs from a GUI (examples 10 and 11)

These are **not Airflow jobs**, so they don't appear on Airflow's Dags page. They're small
desktop programs, standing in for the GUIs our tools already have. Their Run button asks
Airflow to start a job, and the run then appears in the Airflow web page like any other.

```
Your window                          Airflow
[ Run ] ── start job + inputs ──►    runs the job's steps
status  ◄── how is it going? ───     each step's state
```

| File | What it is |
|---|---|
| `10_start_a_job.py` | One Run button that starts example 8 (`batch_run_scripts`) |
| `11_pick_job_and_steps.py` | A "What to run" dropdown: example 8, example 9, or example 11 with step checkboxes |
| `airflow_api.py` | The shared part: log in, start a run, check its status (Airflow's REST API) |

## Try it

With `./run-local.sh` running, in a second terminal in the repo folder:

```bash
source .venv/bin/activate
python3 gui/10_start_a_job.py
python3 gui/11_pick_job_and_steps.py
```
Click **Run**, then open the job in the web page to see the new run.
Add `--no-gui` to print progress in the terminal instead of opening a window.

## Which design to use when a GUI has several options

| The options are… | Use |
|---|---|
| Different procedures | One Airflow job per option; the GUI picks which job to start |
| One procedure with optional steps | One job with an input that picks the steps (example 11) |

## On the server

Set these before running, and make an Airflow account just for the GUI:
```bash
export AIRFLOW_URL=http://<airflow server>:8080
export AIRFLOW_USER=batch_gui
export AIRFLOW_PASSWORD=...
```
Only Python's standard library is used. The window needs tkinter
(on Linux: `apt install python3-tk`); `--no-gui` doesn't.
