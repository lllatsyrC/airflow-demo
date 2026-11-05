"""
Example 10 · Start an Airflow job from another program (a GUI)
==============================================================
Some of our work starts in a desktop GUI, not in a terminal. The GUI doesn't need to
run the scripts itself: its Run button asks Airflow to start the job. Airflow runs it,
and the run shows up in the Airflow web page with its inputs, logs and history, the
same as if someone had used the Trigger form.

    GUI window  --(REST API: start batch_run_scripts with these inputs)-->  Airflow
    GUI window  <--(REST API: is it done yet? which steps passed?)---------  Airflow

This file is NOT a DAG: keep it out of the dags folder. It uses only Python's standard
library (tkinter + urllib), so it runs on the closed network without extra installs.

What it teaches
  - Triggering a DAG through Airflow's REST API, passing the same inputs as the form
  - Following the run: overall state plus each step's state
  - Logging in with a dedicated Airflow account for the GUI, not a person's login

Try it (with ./run-local.sh running)
  python3 gui/trigger_batch_gui.py                      # opens the window
  python3 gui/trigger_batch_gui.py --no-gui \\
      --batch-dir "$PWD/jobs/data/batches/batch_2026_11_04" --no-insert

Settings come from environment variables, so the same script works on the server:
  AIRFLOW_URL       default http://localhost:8080
  AIRFLOW_USER      default admin
  AIRFLOW_PASSWORD  (or it is read from .airflow/simple_auth_manager_passwords.json.generated)

Airflow 3 only (/api/v2 with a login token). On Airflow 2 the paths start with /api/v1,
there is no /auth/token step (send the user and password with each request instead),
and the run body uses "logical_date" only if you want to set one.
"""
import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

DAG_ID = "batch_run_scripts"          # example 8
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080").rstrip("/")
FINISHED = {"success", "failed"}


# ---------------------------------------------------------------------------
# The part any program can reuse: three small calls to Airflow's REST API
# ---------------------------------------------------------------------------
def _password():
    if os.environ.get("AIRFLOW_PASSWORD"):
        return os.environ["AIRFLOW_PASSWORD"]
    # run-local.sh: Airflow standalone saves the generated admin password here
    path = os.path.join(REPO, ".airflow", "simple_auth_manager_passwords.json.generated")
    with open(path) as f:
        return json.load(f)[os.environ.get("AIRFLOW_USER", "admin")]


def _call(method, path, body=None, token=None):
    req = urllib.request.Request(
        AIRFLOW_URL + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json",
                 **({"Authorization": f"Bearer {token}"} if token else {})})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Airflow said {e.code} for {path}: {e.read().decode()[:300]}") from None


def login():
    """Swap a user name and password for a token (Airflow 3)."""
    user = os.environ.get("AIRFLOW_USER", "admin")
    return _call("POST", "/auth/token", {"username": user, "password": _password()})["access_token"]


def start_run(token, inputs):
    """Same as filling in the Trigger form and clicking Trigger. Returns the run id."""
    run = _call("POST", f"/api/v2/dags/{DAG_ID}/dagRuns",
                {"logical_date": None, "conf": inputs}, token)
    return run["dag_run_id"]


def run_status(token, run_id):
    """Overall state ('queued', 'running', 'success', 'failed') and each step's state."""
    rid = urllib.parse.quote(run_id, safe="")
    run = _call("GET", f"/api/v2/dags/{DAG_ID}/dagRuns/{rid}", token=token)
    tis = _call("GET", f"/api/v2/dags/{DAG_ID}/dagRuns/{rid}/taskInstances", token=token)
    # Steps that have started, in the order they started, then the ones still waiting
    ordered = sorted(tis["task_instances"], key=lambda t: (t["start_date"] or "~", t["task_id"]))
    steps = {t["task_id"]: t["state"] or "waiting" for t in ordered}
    return run["state"], steps


def run_link(run_id):
    return f"{AIRFLOW_URL}/dags/{DAG_ID}/runs/{urllib.parse.quote(run_id, safe='')}"


# ---------------------------------------------------------------------------
# Without a window: handy for testing, or for calling from another script
# ---------------------------------------------------------------------------
def run_in_terminal(inputs):
    token = login()
    run_id = start_run(token, inputs)
    print(f"Started {DAG_ID}: {run_id}\n  {run_link(run_id)}")
    while True:
        state, steps = run_status(token, run_id)
        print(f"  {state:8} " + "  ".join(f"{k}={v}" for k, v in steps.items()))
        if state in FINISHED:
            return state
        time.sleep(3)


# ---------------------------------------------------------------------------
# A small window: the kind of GUI our tools already have
# ---------------------------------------------------------------------------
def run_window(default_batch_dir):
    import tkinter as tk
    from tkinter import ttk

    root = tk.Tk()
    root.title("Batch runner (Airflow)")
    frm = ttk.Frame(root, padding=16)
    frm.grid()

    ttk.Label(frm, text="Batch folder").grid(row=0, column=0, sticky="w")
    batch_dir = tk.StringVar(value=default_batch_dir)
    ttk.Entry(frm, textvariable=batch_dir, width=60).grid(row=0, column=1, sticky="we")
    insert = tk.BooleanVar(value=False)
    ttk.Checkbutton(frm, text="Insert into the database", variable=insert).grid(
        row=1, column=1, sticky="w", pady=(6, 0))

    status = tk.StringVar(value="Fill in the batch folder and click Run.")
    ttk.Label(frm, textvariable=status, wraplength=520, justify="left").grid(
        row=3, column=0, columnspan=2, sticky="w", pady=(12, 0))
    link = tk.StringVar()
    ttk.Entry(frm, textvariable=link, width=70, state="readonly").grid(
        row=4, column=0, columnspan=2, sticky="we", pady=(6, 0))
    state = {"token": None, "run_id": None}

    def poll():
        try:
            overall, steps = run_status(state["token"], state["run_id"])
        except Exception as e:  # keep the window alive if Airflow restarts
            status.set(f"Can't reach Airflow: {e}")
            root.after(5000, poll)
            return
        lines = "\n".join(f"   {k}: {v}" for k, v in steps.items())
        status.set(f"Run {overall}\n{lines}")
        if overall in FINISHED:
            run_btn.state(["!disabled"])
        else:
            root.after(3000, poll)

    def on_run():
        run_btn.state(["disabled"])
        try:
            state["token"] = state["token"] or login()
            state["run_id"] = start_run(state["token"], {
                "batch_dir": batch_dir.get().strip(),
                "write_to_db": bool(insert.get()),
            })
        except Exception as e:
            status.set(f"Couldn't start the job: {e}")
            run_btn.state(["!disabled"])
            return
        link.set(run_link(state["run_id"]))
        status.set("Started. Waiting for Airflow...")
        root.after(1000, poll)

    run_btn = ttk.Button(frm, text="Run", command=on_run)
    run_btn.grid(row=2, column=1, sticky="w", pady=(12, 0))
    root.mainloop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--no-gui", action="store_true", help="run in the terminal, no window")
    p.add_argument("--batch-dir",
                   default=os.path.join(REPO, "jobs", "data", "batches", "batch_2026_11_04"))
    p.add_argument("--no-insert", action="store_true", help="skip the database step")
    a = p.parse_args()
    if a.no_gui:
        result = run_in_terminal({"batch_dir": a.batch_dir, "write_to_db": not a.no_insert})
        raise SystemExit(0 if result == "success" else 1)
    run_window(a.batch_dir)
