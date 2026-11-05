"""
Example 8 · Run our existing scripts in order on a batch
========================================================
The pattern for scripts the team runs by hand in a terminal today. A new batch is copied
into a folder by hand; this job takes that folder as an input and runs the scripts in
order, each one's output becoming the next one's input:

    check_inputs >> clean_counts >> summarize_counts >> insert_requested >> load_counts

What it teaches
  - File locations as inputs: a batch folder, plus a file name inside it (or a full path)
  - Scripts that stay where they are: full paths set once at the top of this file,
    even when they live in different folders
  - One shared bash script (jobs/common/run_python_step.sh) reused by every step,
    found through template_searchpath; each task gives it different inputs
  - A step's input coming from the step before it (dependency-based inputs)
  - Running scripts with the team's own Python, not Airflow's
  - An optional database insert: untick "Insert into the database" and the last step
    is skipped
  - Safe reruns: the load script replaces the batch's rows instead of adding duplicates

Setup (once)
  1. Create the table:  mysql -u <admin> -p < jobs/sql/create_batch_tables.sql
  2. Copy jobs/common/db.cnf.example to e.g. /opt/airflow/secrets/team_db.cnf,
     fill in the database login, and chmod 600 it.
  3. The Python that runs the scripts needs pandas and pymysql.

Try it
  airflow dags trigger batch_run_scripts --conf '{
      "batch_dir": "/opt/jobs/data/batches/batch_2026_11_04",
      "db_config": "/opt/airflow/secrets/team_db.cnf"}'
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

# ---------------------------------------------------------------------------
# Where things live on the server. The scripts stay in their own folders; set
# these paths once here (or with the environment variables named below).
# ---------------------------------------------------------------------------
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
RUNS_DIR = os.environ.get("BATCH_RUNS_DIR", "/tmp/airflow_examples/batch_runs")

# Values check_inputs works out, read back inside each step's settings.
CHECKED = "ti.xcom_pull(task_ids='check_inputs')"


def step(task_id, script, in_file, out_file=None, db=False):
    """One step = the shared runner + this step's own inputs."""
    env = {
        "STEP_NAME": task_id,
        "SCRIPT": script,
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
        bash_command="run_python_step.sh",   # found through template_searchpath
        env=env,
        append_env=True,
    )


@dag(
    dag_id="batch_run_scripts",
    dag_display_name="Example 8 · Run our existing scripts in order on a batch",
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
            description="Folder the batch was copied into, e.g. /data/incoming/batch_2026_11_04",
            section="Batch"),
        "counts_file": Param(
            "inventory_counts.csv", type="string", title="Counts file",
            description="File name inside the batch folder, or a full path", section="Batch"),
        "batch_id": Param(
            "", type="string", title="Batch ID",
            description="Label saved with every row. Leave empty to use the folder name.", section="Batch"),
        "write_to_db": Param(
            True, type="boolean", title="Insert into the database",
            description="Untick to clean and summarize only; the load step is skipped.", section="Options"),
        "db_config": Param(
            "/opt/airflow/secrets/team_db.cnf", type="string", title="Database config file",
            description="MySQL option file the load script logs in with (see jobs/common/db.cnf.example)",
            section="Advanced"),
        "python_bin": Param(
            "python3", type="string", title="Python for the team scripts",
            description="e.g. /opt/team/venv/bin/python, so the scripts keep their own packages",
            section="Advanced"),
    },
)
def batch_run_scripts():

    @task(retries=0)  # a missing file won't fix itself
    def check_inputs(**context):
        """Turn the form's paths into full paths, check everything exists, set up a run folder."""
        p = context["params"]
        batch_dir = os.path.abspath(os.path.expanduser(p["batch_dir"].strip()))
        if not os.path.isdir(batch_dir):
            raise AirflowFailException(f"Batch folder not found: {batch_dir}")
        counts = os.path.expanduser(p["counts_file"].strip())
        counts = counts if os.path.isabs(counts) else os.path.join(batch_dir, counts)

        need = {"counts file": counts, **{f"{k} script": v for k, v in SCRIPTS.items()}}
        if p["write_to_db"]:
            need["database config file"] = os.path.expanduser(p["db_config"])
        missing = [f"{what}: {path}" for what, path in need.items() if not os.path.isfile(path)]
        if missing:
            raise AirflowFailException("Not found:\n  " + "\n  ".join(missing))

        batch_id = p["batch_id"].strip() or os.path.basename(batch_dir.rstrip("/"))
        run_dir = os.path.join(RUNS_DIR, batch_id, datetime.now().strftime("%Y%m%d_%H%M%S"))
        os.makedirs(run_dir, exist_ok=True)
        checked = {"batch_id": batch_id, "counts_in": counts, "run_dir": run_dir,
                   "clean_out": os.path.join(run_dir, "counts_clean.csv"),
                   "summary_out": os.path.join(run_dir, "counts_by_store.csv")}
        for k, v in checked.items():
            print(f"{k:<12} {v}")
        return checked

    # Each step: same shared runner, its own script, and its input = the step before's output.
    clean = step("clean_counts", SCRIPTS["clean"],
                 in_file="{{ %s['counts_in'] }}" % CHECKED,
                 out_file="{{ %s['clean_out'] }}" % CHECKED)
    summarize = step("summarize_counts", SCRIPTS["summarize"],
                     in_file="{{ %s['clean_out'] }}" % CHECKED,
                     out_file="{{ %s['summary_out'] }}" % CHECKED)
    load = step("load_counts", SCRIPTS["load"],
                in_file="{{ %s['clean_out'] }}" % CHECKED, db=True)

    @task.short_circuit
    def insert_requested(**context):
        """Returns False when the box is unticked: everything after this is skipped."""
        return context["params"]["write_to_db"]

    check_inputs() >> clean >> summarize >> insert_requested() >> load


batch_run_scripts()
