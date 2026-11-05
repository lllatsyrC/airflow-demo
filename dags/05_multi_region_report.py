"""
Example 5 · One step per selected item

One copy of summarize_region runs per region picked, in parallel, then combine
collects the results. Each copy has its own log and can be rerun alone.
- .expand() over a list from the form (dynamic task mapping)

Try it: airflow dags trigger multi_region_report --conf '{"regions": ["East", "South", "West"], "min_amount": 50}'
"""
import csv
import os
from datetime import datetime

try:  # Airflow 3
    from airflow.sdk import dag, task, Param, get_current_context
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context

JOBS_DIR = os.environ.get(
    "JOBS_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "jobs"))
)
SALES_CSV = os.path.join(JOBS_DIR, "data", "sales.csv")
REGIONS = ["East", "West", "North", "South"]


@dag(
    dag_id="multi_region_report",
    dag_display_name="Example 5 · One step per selected region",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "level-3"],
    params={
        "regions": Param(
            ["East", "West", "North"],
            type="array",
            examples=REGIONS,
            items={"type": "string", "enum": REGIONS},
            minItems=1,
            description="One summary step runs per region you pick",
        ),
        "min_amount": Param(0, type="integer", minimum=0),
        "output_dir": Param("/tmp/airflow_examples/multi_region", type="string"),
    },
)
def multi_region_report():

    @task
    def pick_regions(**context):
        regions = context["params"]["regions"]
        print(f"Will run one step for each of: {regions}")
        return regions

    # map_index_template labels each copy with its region in the UI
    @task(map_index_template="{{ region_label }}")
    def summarize_region(region, **context):
        get_current_context()["region_label"] = region
        min_amount = context["params"]["min_amount"]
        with open(SALES_CSV, newline="") as f:
            amounts = [float(r["amount"]) for r in csv.DictReader(f)
                       if r["region"] == region and float(r["amount"]) >= min_amount]
        result = {"region": region, "rows": len(amounts), "total": round(sum(amounts), 2)}
        print(result)
        return result

    @task
    def combine(results, **context):
        results = sorted(results, key=lambda r: -r["total"])
        out_dir = context["params"]["output_dir"]
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"by_region_{datetime.now():%Y%m%d_%H%M%S}.csv")
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["region", "rows", "total"])
            w.writeheader()
            w.writerows(results)
        print(f"{'Region':<8}{'Rows':>6}{'Total':>12}")
        for r in results:
            print(f"{r['region']:<8}{r['rows']:>6}{r['total']:>12,.2f}")
        print(f"Wrote {path}")

    combine(summarize_region.expand(region=pick_regions()))


multi_region_report()
