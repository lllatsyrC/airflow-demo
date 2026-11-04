"""
Example 3 · Run existing scripts with inputs  (the teach-me demo)
=================================================================
Wraps two scripts we already have. Airflow passes the form values to them
as command-line arguments and runs them in order.

What it teaches
  - BashOperator: run any script, by its full path
  - {{ params.name }}: put a form value into a command
  - Several steps in order: validate >> filter_data >> summarize >> done
  - Passing a value between steps: "validate" returns this run's output
    folder, later steps read it with ti.xcom_pull
  - Scripts live outside the dags folder (see JOBS_DIR)

Try it
  CLI: airflow dags trigger sales_report --conf '{"region": "East", "min_amount": 100}'
  Sample data: East over 100 -> 3 rows, total 610.50
"""
import os
from datetime import datetime

try:  # Airflow 3
    from airflow.sdk import dag, task, Param
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param
    from airflow.operators.bash import BashOperator

# Scripts and data live OUTSIDE the dags folder. Set JOBS_DIR on the server
# (e.g. /opt/jobs); the fallback is the jobs/ folder next to dags/.
JOBS_DIR = os.environ.get(
    "JOBS_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "jobs"))
)
SCRIPTS = os.path.join(JOBS_DIR, "scripts")
DATA = os.path.join(JOBS_DIR, "data")

# The folder "validate" returned, read back inside a command.
RUN_DIR = "{{ ti.xcom_pull(task_ids='validate') }}"


@dag(
    dag_id="sales_report",
    dag_display_name="Example 3 · Sales report (runs our scripts)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "level-2", "demo"],
    params={
        "region": Param("East", enum=["East", "West", "North", "South", "All"],
                        description="Which region to report on"),
        "min_amount": Param(0, type="integer", minimum=0,
                            description="Ignore sales below this amount"),
        "output_dir": Param("/tmp/airflow_examples/sales_report", type="string",
                            description="Folder for the output files"),
        "dry_run": Param(False, type="boolean",
                         description="If ticked, only print what would happen"),
    },
)
def sales_report():

    @task
    def validate(**context):
        """Check the input file exists and make a folder for this run's files."""
        p = context["params"]
        print(f"Running with inputs: {p}")
        if not os.path.exists(os.path.join(DATA, "sales.csv")):
            raise FileNotFoundError(f"Missing input file in {DATA}")
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = os.path.join(p["output_dir"], f"{p['region']}_{stamp}")
        os.makedirs(run_dir, exist_ok=True)
        return run_dir          # passed to the next steps

    filter_data = BashOperator(
        task_id="filter_data",
        bash_command=(
            f"python {SCRIPTS}/filter_data.py "
            f"--input {DATA}/sales.csv "
            f"--output {RUN_DIR}/filtered.csv "
            "--region {{ params.region }} "
            "--min-amount {{ params.min_amount }} "
            "{{ '--dry-run' if params.dry_run else '' }}"
        ),
    )

    summarize = BashOperator(
        task_id="summarize",
        bash_command=(
            f"python {SCRIPTS}/summarize.py "
            f"--input {RUN_DIR}/filtered.csv "
            f"--output {RUN_DIR}/summary.txt "
            "{{ '--dry-run' if params.dry_run else '' }}"
        ),
    )

    @task
    def done(run_dir):
        print(f"Finished. Results are in {run_dir}")

    run_dir = validate()
    run_dir >> filter_data >> summarize >> done(run_dir)


sales_report()
