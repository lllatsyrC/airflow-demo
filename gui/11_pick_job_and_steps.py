"""
Example 11 · A GUI with several run options
===========================================
Like example 10, but the window offers several things to run. Two ways to wire that up,
both shown here:

  1. Each option starts a different Airflow job      ("Process a batch", "Load trips")
  2. One job, and the GUI's checkboxes pick its steps ("Pick steps" -> example 11's job)

The OPTIONS table below is the only place that knows which button starts which job.
To add an option, add a row.

Try it (with ./run-local.sh running, in a second terminal):
  python3 gui/11_pick_job_and_steps.py
  python3 gui/11_pick_job_and_steps.py --no-gui --option steps --steps clean summarize
"""
import argparse
import os
import time

from airflow_api import FINISHED, REPO, login, run_link, run_status, start_run

SAMPLE_BATCH = os.path.join(REPO, "jobs", "data", "batches", "batch_2026_11_04")
STEPS = [("clean", "Clean the counts"), ("summarize", "Summarize by store"), ("load", "Load into the database")]

# option key -> (label in the window, Airflow job it starts, how to build that job's inputs)
OPTIONS = {
    "batch": ("Process a batch (example 8)", "batch_run_scripts",
              lambda folder, insert, steps: {"batch_dir": folder, "write_to_db": insert}),
    "trips": ("Load trips into the database (example 9)", "batch_load_trips",
              lambda folder, insert, steps: {"batch_dir": folder}),
    "steps": ("Pick steps (example 11)", "batch_pick_steps",
              lambda folder, insert, steps: {"batch_dir": folder, "steps": steps}),
}
USES_INSERT = {"batch"}   # options where the Insert checkbox means something
USES_STEPS = {"steps"}    # options where the step checkboxes mean something


def run_in_terminal(option, folder, insert, steps):
    label, dag_id, build = OPTIONS[option]
    token = login()
    run_id = start_run(token, dag_id, build(folder, insert, steps))
    print(f"{label}: started {dag_id}\n  {run_link(dag_id, run_id)}")
    while True:
        state, by_step = run_status(token, dag_id, run_id)
        print(f"  {state:8} " + "  ".join(f"{k}={v}" for k, v in by_step.items()))
        if state in FINISHED:
            return state
        time.sleep(3)


# mock GUI
def run_window():
    import tkinter as tk
    from tkinter import ttk

    root = tk.Tk()
    root.title("Batch tools (Airflow)")
    frm = ttk.Frame(root, padding=16)
    frm.grid()

    ttk.Label(frm, text="What to run").grid(row=0, column=0, sticky="w")
    labels = {v[0]: k for k, v in OPTIONS.items()}
    choice = tk.StringVar(value=OPTIONS["batch"][0])
    ttk.Combobox(frm, textvariable=choice, values=list(labels), state="readonly", width=45).grid(
        row=0, column=1, sticky="w")

    ttk.Label(frm, text="Batch folder").grid(row=1, column=0, sticky="w", pady=(8, 0))
    folder = tk.StringVar(value=SAMPLE_BATCH)
    ttk.Entry(frm, textvariable=folder, width=60).grid(row=1, column=1, sticky="we", pady=(8, 0))

    insert = tk.BooleanVar(value=False)
    insert_box = ttk.Checkbutton(frm, text="Insert into the database", variable=insert)
    insert_box.grid(row=2, column=1, sticky="w", pady=(8, 0))

    steps_frame = ttk.LabelFrame(frm, text="Steps", padding=8)
    steps_frame.grid(row=3, column=1, sticky="w", pady=(8, 0))
    step_vars = {}
    for i, (key, text) in enumerate(STEPS):
        step_vars[key] = tk.BooleanVar(value=key != "load")
        ttk.Checkbutton(steps_frame, text=text, variable=step_vars[key]).grid(row=0, column=i, padx=(0, 12))

    def show_controls(*_):
        key = labels[choice.get()]
        insert_box.state(["!disabled"] if key in USES_INSERT else ["disabled"])
        for w in steps_frame.winfo_children():
            w.state(["!disabled"] if key in USES_STEPS else ["disabled"])
    choice.trace_add("write", show_controls)
    show_controls()

    status = tk.StringVar(value="Pick what to run, then click Run.")
    ttk.Label(frm, textvariable=status, justify="left").grid(row=5, column=0, columnspan=2, sticky="w", pady=(12, 0))
    link = tk.StringVar()
    ttk.Entry(frm, textvariable=link, width=70, state="readonly").grid(
        row=6, column=0, columnspan=2, sticky="we", pady=(6, 0))
    state = {"token": None, "run_id": None, "dag_id": None}

    def poll():
        try:
            overall, by_step = run_status(state["token"], state["dag_id"], state["run_id"])
        except Exception as e:
            status.set(f"Can't reach Airflow: {e}")
            root.after(5000, poll)
            return
        status.set(f"{state['dag_id']}: {overall}\n" + "\n".join(f"   {k}: {v}" for k, v in by_step.items()))
        if overall in FINISHED:
            run_btn.state(["!disabled"])
        else:
            root.after(3000, poll)

    def on_run():
        key = labels[choice.get()]
        _, dag_id, build = OPTIONS[key]
        picked = [k for k, _ in STEPS if step_vars[k].get()]
        if key in USES_STEPS and not picked:
            status.set("Tick at least one step.")
            return
        run_btn.state(["disabled"])
        try:
            state["token"] = state["token"] or login()
            state["run_id"] = start_run(state["token"], dag_id, build(folder.get().strip(), bool(insert.get()), picked))
            state["dag_id"] = dag_id
        except Exception as e:
            status.set(f"Couldn't start the job: {e}")
            run_btn.state(["!disabled"])
            return
        link.set(run_link(dag_id, state["run_id"]))
        status.set(f"Started {dag_id}. Waiting for Airflow...")
        root.after(1000, poll)

    run_btn = ttk.Button(frm, text="Run", command=on_run)
    run_btn.grid(row=4, column=1, sticky="w", pady=(12, 0))
    root.mainloop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Example 11: a window with several run options")
    p.add_argument("--no-gui", action="store_true", help="run in the terminal, no window")
    p.add_argument("--option", choices=list(OPTIONS), default="batch")
    p.add_argument("--batch-dir", default=SAMPLE_BATCH)
    p.add_argument("--insert", action="store_true", help="example 8: insert into the database")
    p.add_argument("--steps", nargs="+", choices=[k for k, _ in STEPS], default=["clean", "summarize"])
    a = p.parse_args()
    if a.no_gui:
        result = run_in_terminal(a.option, a.batch_dir, a.insert, a.steps)
        raise SystemExit(0 if result == "success" else 1)
    run_window()
