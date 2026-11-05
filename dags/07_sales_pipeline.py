"""
Example 7 · Everything together: a production-style manual job

Examples 1-6 combined, plus the habits that make a job safe to hand to others:
- required "Your name" input, recorded in a manifest.json with the inputs
- validation that fails fast; retries on the other steps
- a task group that fans out per region
- skips cleanly when nothing matches
- a failure callback (writes failures.log; swap in email/chat)
- optionally starts another job (sales_report) with its own inputs
- max_active_runs=1: one run at a time

Try it: airflow dags trigger sales_pipeline --conf '{"requested_by": "Alex", "regions": ["East", "West"]}'
"""
import csv
import json
import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import dag, task, task_group, Param, get_current_context
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task, task_group
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException, AirflowSkipException

JOBS_DIR = os.environ.get(
    "JOBS_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "jobs"))
)
SALES_CSV = os.path.join(JOBS_DIR, "data", "sales.csv")
REGIONS = ["East", "West", "North", "South"]


def notify_failure(context):
    """Runs when a task fails for good. Logs to failures.log; swap in email or chat."""
    ti = context["task_instance"]
    msg = (f"{datetime.now():%Y-%m-%d %H:%M} FAILED {ti.dag_id}.{ti.task_id} "
           f"run={context['run_id']} requested_by={context['params'].get('requested_by')}")
    print(msg)
    out_dir = context["params"].get("output_dir", "/tmp")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "failures.log"), "a") as f:
        f.write(msg + "\n")


@dag(
    dag_id="sales_pipeline",
    dag_display_name="Example 7 · Sales pipeline (everything together)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["example", "level-4"],
    doc_md=__doc__,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": notify_failure,
    },
    params={
        "requested_by": Param(type="string", minLength=2, title="Your name",
                              section="Who and why"),
        "run_note": Param("", type="string", format="multiline", title="Reason / note",
                          section="Who and why"),
        "start_date": Param("2026-10-01", type="string", format="date", title="From",
                            section="What to report"),
        "end_date": Param("2026-10-31", type="string", format="date", title="To (inclusive)",
                          section="What to report"),
        "regions": Param(REGIONS, type="array", examples=REGIONS,
                         items={"type": "string", "enum": REGIONS}, minItems=1,
                         title="Regions", section="What to report"),
        "min_amount": Param(0, type="number", minimum=0, title="Minimum amount",
                            section="What to report"),
        "formats": Param(["csv", "txt"], type="array", examples=["csv", "txt", "json"],
                         items={"type": "string", "enum": ["csv", "txt", "json"]},
                         minItems=1, title="Output formats", section="Output"),
        "output_dir": Param("/tmp/airflow_examples/pipeline", type="string",
                            title="Output folder", section="Output"),
        "dry_run": Param(False, type="boolean", title="Dry run (write nothing)",
                         section="Options"),
        "run_standard_report": Param(False, type="boolean",
                                     title="Also start the standard sales_report job",
                                     section="Options"),
    },
)
def sales_pipeline():

    @task(retries=0)  # retrying won't fix bad input
    def validate(**context):
        p = context["params"]
        problems = []
        if p["end_date"] < p["start_date"]:
            problems.append(f"'To' {p['end_date']} is before 'From' {p['start_date']}")
        if not os.path.exists(SALES_CSV):
            problems.append(f"input file missing: {SALES_CSV}")
        if problems:
            raise AirflowFailException("; ".join(problems))
        run_dir = os.path.join(p["output_dir"], datetime.now().strftime("%Y%m%d_%H%M%S"))
        print(f"Inputs OK. Output folder: {run_dir}")
        return run_dir

    @task_group(group_id="extract")
    def extract(regions):

        @task(map_index_template="{{ region_label }}")
        def extract_region(region, **context):
            get_current_context()["region_label"] = region
            p = context["params"]
            # swap for example 6's MySqlHook query to read from the database
            with open(SALES_CSV, newline="") as f:
                rows = [r for r in csv.DictReader(f)
                        if r["region"] == region
                        and p["start_date"] <= r["date"] <= p["end_date"]
                        and float(r["amount"]) >= p["min_amount"]]
            print(f"{region}: {len(rows)} rows")
            return rows

        return extract_region.expand(region=regions)

    @task
    def get_regions(**context):
        return context["params"]["regions"]

    @task
    def build_report(per_region, run_dir, **context):
        p = context["params"]
        rows = [r for region_rows in per_region for r in region_rows]
        if not rows:
            raise AirflowSkipException("No sales match these inputs; nothing to report.")

        totals = {}
        for r in rows:
            totals[r["region"]] = totals.get(r["region"], 0) + float(r["amount"])
        lines = [f"{region:<8}{total:>12,.2f}" for region, total in sorted(totals.items())]
        lines.append(f"{'TOTAL':<8}{sum(totals.values()):>12,.2f}")
        print("\n".join(lines))

        if p["dry_run"]:
            print(f"[dry run] would write {p['formats']} to {run_dir}")
            return run_dir

        os.makedirs(run_dir, exist_ok=True)
        if "csv" in p["formats"]:
            with open(os.path.join(run_dir, "sales.csv"), "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["date", "region", "product", "amount"])
                w.writeheader()
                w.writerows(rows)
        if "txt" in p["formats"]:
            with open(os.path.join(run_dir, "summary.txt"), "w") as f:
                f.write("\n".join(lines) + "\n")
        if "json" in p["formats"]:
            with open(os.path.join(run_dir, "totals.json"), "w") as f:
                json.dump(totals, f, indent=2)

        manifest = {
            "job": context["dag"].dag_id,
            "run_id": context["run_id"],
            "requested_by": p["requested_by"],
            "note": p["run_note"],
            "inputs": dict(p),
            "row_count": len(rows),
            "written_at": datetime.now().isoformat(timespec="seconds"),
        }
        with open(os.path.join(run_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"Wrote {sorted(os.listdir(run_dir))} to {run_dir}")
        return run_dir

    @task.short_circuit
    def wants_followup(**context):
        p = context["params"]
        return p["run_standard_report"] and not p["dry_run"]

    @task
    def followup_inputs(**context):
        return {"region": "All", "min_amount": int(context["params"]["min_amount"])}

    run_dir = validate()
    regions = get_regions()
    run_dir >> regions
    report = build_report(extract(regions), run_dir)

    conf = followup_inputs()
    start_sales_report = TriggerDagRunOperator(
        task_id="start_sales_report",
        trigger_dag_id="sales_report",
        conf=conf,
        wait_for_completion=False,
    )
    report >> wants_followup() >> conf >> start_sales_report


sales_pipeline()
