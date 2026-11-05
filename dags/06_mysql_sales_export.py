"""
Example 6 · Query MySQL with the form's inputs

Exports sales for a date range and regions to CSV, then prints totals.
- AirflowFailException: stop at once on bad input, no retries
- MySqlHook with an Airflow Connection: no passwords in the code
- Inputs go into the query as %s parameters, never pasted into the SQL

Setup: see dags/README.md (sample table + mysql_default connection).
Try it: airflow dags trigger mysql_sales_export --conf '{"start_date": "2026-10-02", "end_date": "2026-10-04"}'
"""
import csv
import os
from datetime import datetime

try:  # Airflow 3
    from airflow.sdk import dag, task, Param
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param
from airflow.exceptions import AirflowFailException

REGIONS = ["East", "West", "North", "South"]


@dag(
    dag_id="mysql_sales_export",
    dag_display_name="Example 6 · Query MySQL with the form's inputs",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "level-3", "mysql"],
    params={
        "start_date": Param("2026-10-01", type="string", format="date",
                            title="From", section="What to export"),
        "end_date": Param("2026-10-31", type="string", format="date",
                          title="To (inclusive)", section="What to export"),
        "regions": Param(REGIONS, type="array", examples=REGIONS,
                         items={"type": "string", "enum": REGIONS}, minItems=1,
                         title="Regions", section="What to export"),
        "min_amount": Param(0, type="number", minimum=0,
                            title="Minimum amount", section="What to export"),
        "output_dir": Param("/tmp/airflow_examples/mysql_export", type="string",
                            title="Output folder", section="Advanced"),
        "mysql_conn_id": Param("mysql_default", type="string",
                               title="Airflow connection to use", section="Advanced"),
    },
)
def mysql_sales_export():

    @task
    def check_inputs(**context):
        p = context["params"]
        if p["end_date"] < p["start_date"]:
            raise AirflowFailException(
                f"'To' ({p['end_date']}) is before 'From' ({p['start_date']})")
        print(f"Exporting {p['regions']} from {p['start_date']} to {p['end_date']}")

    @task
    def query_sales(**context):
        # imported here so the job still loads where the provider isn't installed
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        p = context["params"]
        placeholders = ", ".join(["%s"] * len(p["regions"]))
        sql = f"""
            SELECT sale_date, region, product, amount
            FROM sales
            WHERE sale_date BETWEEN %s AND %s
              AND region IN ({placeholders})
              AND amount >= %s
            ORDER BY sale_date, region
        """
        values = [p["start_date"], p["end_date"], *p["regions"], p["min_amount"]]
        rows = MySqlHook(mysql_conn_id=p["mysql_conn_id"]).get_records(sql, parameters=values)

        os.makedirs(p["output_dir"], exist_ok=True)
        path = os.path.join(p["output_dir"], f"sales_{datetime.now():%Y%m%d_%H%M%S}.csv")
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["date", "region", "product", "amount"])
            w.writerows(rows)
        print(f"{len(rows)} rows -> {path}")
        return path

    @task
    def print_totals(path):
        totals = {}
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                totals[r["region"]] = totals.get(r["region"], 0) + float(r["amount"])
        for region, total in sorted(totals.items()):
            print(f"  {region:<8}{total:>12,.2f}")
        print(f"  {'TOTAL':<8}{sum(totals.values()):>12,.2f}")

    check_inputs() >> print_totals(query_sales())


mysql_sales_export()
