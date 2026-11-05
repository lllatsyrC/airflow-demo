"""
Example 10 · Start an Airflow job from a GUI
============================================
A small window with one Run button. Clicking it asks Airflow to start example 8
(batch_run_scripts) with the batch folder typed in the window, then shows each step's
progress. The run also appears under Example 8 in the Airflow web page, with its logs.

This is a normal Python program, not an Airflow job: it stays out of the dags folder.

Try it (with ./run-local.sh running, in a second terminal):
  python3 gui/10_start_a_job.py                         # the window
  python3 gui/10_start_a_job.py --no-gui --no-insert    # the same, printed in the terminal
"""
import argparse
import os
import time

from airflow_api import FINISHED, REPO, login, run_link, run_status, start_run

DAG_ID = "batch_run_scripts"   # example 8
SAMPLE_BATCH = os.path.join(REPO, "jobs", "data", "batches", "batch_2026_11_04")


def run_in_terminal(inputs):
    token = login()
    run_id = start_run(token, DAG_ID, inputs)
    print(f"Started {DAG_ID}: {run_id}\n  {run_link(DAG_ID, run_id)}")
    while True:
        state, steps = run_status(token, DAG_ID, run_id)
        print(f"  {state:8} " + "  ".join(f"{k}={v}" for k, v in steps.items()))
        if state in FINISHED:
            return state
        time.sleep(3)


# mock GUI
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
    ttk.Label(frm, textvariable=status, justify="left").grid(row=3, column=0, columnspan=2, sticky="w", pady=(12, 0))
    link = tk.StringVar()
    ttk.Entry(frm, textvariable=link, width=70, state="readonly").grid(
        row=4, column=0, columnspan=2, sticky="we", pady=(6, 0))
    state = {"token": None, "run_id": None}

    def poll():
        try:
            overall, steps = run_status(state["token"], DAG_ID, state["run_id"])
        except Exception as e:  # keep the window alive if Airflow restarts
            status.set(f"Can't reach Airflow: {e}")
            root.after(5000, poll)
            return
        status.set(f"Run {overall}\n" + "\n".join(f"   {k}: {v}" for k, v in steps.items()))
        if overall in FINISHED:
            run_btn.state(["!disabled"])
        else:
            root.after(3000, poll)

    def on_run():
        run_btn.state(["disabled"])
        try:
            state["token"] = state["token"] or login()
            state["run_id"] = start_run(state["token"], DAG_ID, {
                "batch_dir": batch_dir.get().strip(),
                "write_to_db": bool(insert.get()),
            })
        except Exception as e:
            status.set(f"Couldn't start the job: {e}")
            run_btn.state(["!disabled"])
            return
        link.set(run_link(DAG_ID, state["run_id"]))
        status.set("Started. Waiting for Airflow...")
        root.after(1000, poll)

    run_btn = ttk.Button(frm, text="Run", command=on_run)
    run_btn.grid(row=2, column=1, sticky="w", pady=(12, 0))
    root.mainloop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Example 10: start example 8 from a window")
    p.add_argument("--no-gui", action="store_true", help="run in the terminal, no window")
    p.add_argument("--batch-dir", default=SAMPLE_BATCH)
    p.add_argument("--no-insert", action="store_true", help="skip the database step")
    a = p.parse_args()
    if a.no_gui:
        raise SystemExit(0 if run_in_terminal({"batch_dir": a.batch_dir, "write_to_db": not a.no_insert}) == "success" else 1)
    run_window(a.batch_dir)
