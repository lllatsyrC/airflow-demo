"""
Example 4 · Let the inputs choose the path
==========================================
The form decides which steps run. Steps that aren't chosen show as
"skipped" (pink) in the UI.

What it teaches
  - @task.branch: return the task_id(s) to run next
  - A dropdown input that changes what the job does
  - trigger_rule on the last step so it still runs after a skipped branch
  - Passing small data between Python tasks (return value -> argument)

Try it
  CLI: airflow dags trigger report_branching --conf '{"report_type": "both"}'
       airflow dags trigger report_branching --conf '{"dry_run": true}'
"""
import csv
import os
from collections import defaultdict
from datetime import datetime

try:  # Airflow 3
    from airflow.sdk import dag, task, Param
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param

JOBS_DIR = os.environ.get(
    "JOBS_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "jobs"))
)
SALES_CSV = os.path.join(JOBS_DIR, "data", "sales.csv")


@dag(
    dag_id="report_branching",
    dag_display_name="Example 4 · Let the inputs choose the path",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "level-2"],
    params={
        "region": Param("All", enum=["All", "East", "West", "North", "South"]),
        "report_type": Param(
            "summary",
            enum=["summary", "detail", "both"],
            values_display={"summary": "Summary (totals by product)",
                            "detail": "Detail (every row)",
                            "both": "Both"},
            description="Which report to produce",
        ),
        "dry_run": Param(False, type="boolean",
                         description="Only preview the matching rows"),
    },
)
def report_branching():

    @task
    def load_rows(**context):
        region = context["params"]["region"]
        with open(SALES_CSV, newline="") as f:
            rows = [r for r in csv.DictReader(f)
                    if region == "All" or r["region"] == region]
        print(f"{len(rows)} rows for region={region}")
        # Fine for small data. For big data, write a file and return its path.
        return rows

    @task.branch
    def choose_report(**context):
        p = context["params"]
        if p["dry_run"]:
            return "dry_run_preview"
        return {
            "summary": ["make_summary"],
            "detail": ["make_detail"],
            "both": ["make_summary", "make_detail"],
        }[p["report_type"]]

    @task
    def make_summary(rows):
        totals = defaultdict(float)
        for r in rows:
            totals[r["product"]] += float(r["amount"])
        print("SUMMARY by product")
        for product, total in sorted(totals.items(), key=lambda x: -x[1]):
            print(f"  {product:<10} {total:>10,.2f}")

    @task
    def make_detail(rows):
        print("DETAIL")
        for r in rows:
            print(f"  {r['date']}  {r['region']:<6} {r['product']:<8} {float(r['amount']):>9,.2f}")

    @task
    def dry_run_preview(rows):
        print(f"[dry run] would report on {len(rows)} rows; nothing written")

    # Runs if at least one branch succeeded and none failed.
    @task(trigger_rule="none_failed_min_one_success")
    def finish():
        print("Done.")

    rows = load_rows()
    branches = [make_summary(rows), make_detail(rows), dry_run_preview(rows)]
    rows >> choose_report() >> branches >> finish()


report_branching()
