"""
Example 11 · Pick which steps run

Example 8's batch job, but you tick which steps to run: clean, summarize, load.
Steps not picked show as skipped (pink).
- exit code 99 from a bash step means "skipped", not "failed"
- trigger_rule="none_failed" lets a step run after a skipped one
- check_inputs makes sure the picked steps can work before anything runs
  (summarize on its own needs a cleaned file from an earlier run)

Try it: airflow dags trigger batch_pick_steps --conf '{"batch_dir": "/opt/jobs/data/batches/batch_2026_11_04", "steps": ["clean", "summarize"]}'
Also started from a window by gui/11_pick_job_and_steps.py.
"""
import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import dag, task, Param
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param
    from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException

JOBS_DIR = os.environ.get(
    "JOBS_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "jobs"))
)
SCRIPTS = {
    "clean": os.environ.get(
        "CLEAN_COUNTS_SCRIPT", os.path.join(JOBS_DIR, "team", "inventory", "clean_counts.py")),
    "summarize": os.environ.get(
        "SUMMARIZE_COUNTS_SCRIPT", os.path.join(JOBS_DIR, "shared", "reporting", "summarize_counts.py")),
    "load": os.environ.get(
        "LOAD_COUNTS_SCRIPT", os.path.join(JOBS_DIR, "team", "inventory", "load_counts.py")),
}
COMMON_BASH_DIR = os.environ.get("COMMON_BASH_DIR", os.path.join(JOBS_DIR, "common"))
WORK_DIR = os.environ.get("BATCH_WORK_DIR", "/tmp/airflow_examples/batch_work")
STEP_NAMES = ["clean", "summarize", "load"]
CHECKED = "ti.xcom_pull(task_ids='check_inputs')"


def step(name, task_id, in_file, out_file=None, db=False, after_skip_ok=False):
    """Run the shared runner if this step was picked; otherwise exit 99 (= skipped)."""
    env = {
        "STEP_NAME": task_id,
        "SCRIPT": SCRIPTS[name],
        "IN_FILE": in_file,
        "BATCH_ID": "{{ %s['batch_id'] }}" % CHECKED,
        "PYTHON_BIN": "{{ params.python_bin }}",
    }
    if out_file:
        env["OUT_FILE"] = out_file
    if db:
        env["DB_CONFIG"] = "{{ params.db_config }}"
    return BashOperator(
        task_id=task_id,
        bash_command=(
            "{%% if '%s' in params.steps %%}{%% include 'run_python_step.sh' %%}"
            "{%% else %%}echo 'Not picked in this run: skipping.'; exit 99{%% endif %%}" % name
        ),
        env=env,
        append_env=True,
        trigger_rule="none_failed" if after_skip_ok else "all_success",
    )


@dag(
    dag_id="batch_pick_steps",
    dag_display_name="Example 11 · Pick which steps run",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "level-3", "batch"],
    doc_md=__doc__,
    template_searchpath=[COMMON_BASH_DIR],
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    params={
        "batch_dir": Param(
            type="string", minLength=1, title="Batch folder",
            description="Folder the batch was copied into", section="Batch"),
        "steps": Param(
            ["clean", "summarize"], type="array", title="Steps to run", examples=STEP_NAMES,
            items={"type": "string", "enum": STEP_NAMES},
            values_display={"clean": "Clean the counts", "summarize": "Summarize by store",
                            "load": "Load into the database"},
            description="Pick one or more. Summarize or load on their own reuse the last cleaned file for this batch.",
            section="Batch"),
        "db_config": Param(
            "/opt/airflow/secrets/team_db.cnf", type="string", title="Database config file",
            description="Only needed when Load is picked", section="Advanced"),
        "python_bin": Param(
            "python3", type="string", title="Python for the team scripts", section="Advanced"),
    },
)
def batch_pick_steps():

    @task(retries=0)
    def check_inputs(**context):
        """Check the picked steps can all work before any of them runs."""
        p = context["params"]
        steps = set(p["steps"])
        if not steps:
            raise AirflowFailException("No steps picked.")
        batch_dir = os.path.abspath(os.path.expanduser(p["batch_dir"].strip()))
        if not os.path.isdir(batch_dir):
            raise AirflowFailException(f"Batch folder not found: {batch_dir}")
        batch_id = os.path.basename(batch_dir.rstrip("/"))
        work = os.path.join(WORK_DIR, batch_id)
        os.makedirs(work, exist_ok=True)
        checked = {"batch_id": batch_id,
                   "counts_in": os.path.join(batch_dir, "inventory_counts.csv"),
                   "clean_out": os.path.join(work, "counts_clean.csv"),
                   "summary_out": os.path.join(work, "counts_by_store.csv")}

        need = {f"{s} script": SCRIPTS[s] for s in steps}
        if "clean" in steps:
            need["counts file"] = checked["counts_in"]
        elif steps & {"summarize", "load"}:
            need["cleaned file from an earlier run (pick Clean too)"] = checked["clean_out"]
        if "load" in steps:
            need["database config file"] = os.path.expanduser(p["db_config"])
        missing = [f"{what}: {path}" for what, path in need.items() if not os.path.isfile(path)]
        if missing:
            raise AirflowFailException("Not found:\n  " + "\n  ".join(missing))

        print("Steps picked:", ", ".join(s for s in STEP_NAMES if s in steps))
        for k, v in checked.items():
            print(f"{k:<12} {v}")
        return checked

    clean = step("clean", "clean_counts",
                 in_file="{{ %s['counts_in'] }}" % CHECKED,
                 out_file="{{ %s['clean_out'] }}" % CHECKED)
    summarize = step("summarize", "summarize_counts",
                     in_file="{{ %s['clean_out'] }}" % CHECKED,
                     out_file="{{ %s['summary_out'] }}" % CHECKED, after_skip_ok=True)
    load = step("load", "load_counts",
                in_file="{{ %s['clean_out'] }}" % CHECKED, db=True, after_skip_ok=True)

    check_inputs() >> clean >> summarize >> load


batch_pick_steps()
