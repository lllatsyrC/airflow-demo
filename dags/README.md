# The example jobs

Each file starts with a short note on what it shows and how to try it.
In the web page they're named "Example 1 · …" and tagged `example`.

| # | File | Job (dag_id) | Shows |
|---|---|---|---|
| 1 | `01_hello_manual.py` | `hello_manual` | `schedule=None`, one input, reading it in a task |
| 2 | `02_form_inputs.py` | `form_inputs_tour` | Every input type: dropdown, multi-select, date, checkbox, limits, required fields, sections |
| 3 | `03_sales_report.py` | `sales_report` | Running existing scripts with the inputs; steps in order; passing a value between steps |
| 4 | `04_report_branching.py` | `report_branching` | An input picks which steps run; skipped steps |
| 5 | `05_multi_region_report.py` | `multi_region_report` | One parallel step per region picked (`.expand`) |
| 6 | `06_mysql_sales_export.py` | `mysql_sales_export` | MySQL with an Airflow connection; safe query parameters |
| 7 | `07_sales_pipeline.py` | `sales_pipeline` | Retries, failure alerts, run manifest, skip when empty, starting another job |
| 8 | `08_run_scripts.py` | `batch_run_scripts` | Our batch scripts in order, from their own folders, via one shared bash runner; optional DB load |
| 9 | `09_two_table_load.py` | `batch_load_trips` | Two Python tasks, one per table; task 2 reads task 1's rows from the DB |
| 11 | `11_pick_steps.py` | `batch_pick_steps` | Tick which of example 8's steps run |

Example 10 has no job of its own: it's a GUI that starts example 8 (see [../gui](../gui/README.md)).

## Setup on a server

```bash
export AIRFLOW_HOME=/opt/airflow
export JOBS_DIR=/opt/jobs                 # where the jobs/ folder lives
cp dags/*.py $AIRFLOW_HOME/dags/
airflow dags list-import-errors           # should print "No data found"
```
Set `JOBS_DIR` for the scheduler and workers too. New jobs start **paused**: switch them on in the web page.

**Example 6** needs the sample table, a connection and the MySQL provider (`pip install "apache-airflow[mysql]"`):
```bash
mysql -u <admin> -p < jobs/sql/create_sample_sales.sql
airflow connections add mysql_default --conn-type mysql --conn-host <host> --conn-port 3306 \
  --conn-schema airflow_examples --conn-login <user> --conn-password '<password>'
```

**Examples 8, 9 and 11** need the batch tables:
```bash
mysql -u <admin> -p < jobs/sql/create_batch_tables.sql
```
- 8 and 11 run scripts outside Airflow, so the load script logs in with a MySQL option file:
  copy `jobs/common/db.cnf.example` to e.g. `/opt/airflow/secrets/team_db.cnf`, fill it in, `chmod 600` it.
  The Python that runs the scripts needs `pandas` and `pymysql`.
- 9 runs inside Airflow: it uses the `mysql_default` connection, and Airflow's Python needs `pandas`.
- Script paths are set at the top of `08_run_scripts.py` / `11_pick_steps.py`
  (or with `CLEAN_COUNTS_SCRIPT`, `SUMMARIZE_COUNTS_SCRIPT`, `LOAD_COUNTS_SCRIPT`, `COMMON_BASH_DIR`).

## Running from the command line

```bash
airflow dags trigger hello_manual        --conf '{"name": "Alex"}'
airflow dags trigger sales_report        --conf '{"region": "East", "min_amount": 100}'
airflow dags trigger report_branching    --conf '{"report_type": "both"}'
airflow dags trigger multi_region_report --conf '{"regions": ["East", "South"], "min_amount": 50}'
airflow dags trigger mysql_sales_export  --conf '{"start_date": "2026-10-02", "end_date": "2026-10-04"}'
airflow dags trigger sales_pipeline      --conf '{"requested_by": "Alex", "regions": ["East", "West"]}'
airflow dags trigger batch_run_scripts   --conf '{"batch_dir": "/opt/jobs/data/batches/batch_2026_11_04", "db_config": "/opt/airflow/secrets/team_db.cnf"}'
airflow dags trigger batch_load_trips    --conf '{"batch_dir": "/opt/jobs/data/batches/batch_2026_11_04"}'
airflow dags trigger batch_pick_steps    --conf '{"batch_dir": "/opt/jobs/data/batches/batch_2026_11_04", "steps": ["clean", "summarize"]}'
```
Inputs left out keep their defaults; required ones (examples 2, 7, 8, 9, 11) must be given.
To run once in the terminal without the scheduler: `airflow dags test <job> -c '<json>'`.
A new job file not showing yet? `airflow dags reserialize` loads it right away.

## Expected results on the sample data

| Run | Result |
|---|---|
| `sales_report` East, min 100 | 3 rows, total 610.50 |
| `multi_region_report` East/South/West, min 50 | East 706.25, West 475.00, South 350.00 |
| `mysql_sales_export` 2026-10-02 to 10-04, East + West | 4 rows, total 985.75 |
| `sales_pipeline` East + West, 2026-10-01 to 10-04 | 6 rows, total 1,181.25 |
| `sales_pipeline` To before From | Fails at once, no retries, line added to `failures.log` |
| `batch_run_scripts` | 7 raw → 5 rows; by store: Riverside 30, Northgate 19, Hillcrest 14 |
| `batch_run_scripts` insert unticked | `load_counts` skipped, nothing inserted |
| `batch_load_trips` | trips 6 → 4 rows; trip_points 9 → 8 rows |
| `batch_pick_steps` clean + summarize | `load_counts` skipped |
| `batch_pick_steps` summarize only, batch never cleaned | Fails at `check_inputs`: "pick Clean too" |
| Any batch job, same batch again | Same row counts: the batch's rows are replaced, not duplicated |

## Patterns to copy

- **Inputs as a form:** `params={...}` with `Param(...)` (example 2). Read with `context["params"]["x"]` or `{{ params.x }}`.
- **Required input:** no default, `Param(type="string")`.
- **Stop on bad input without retrying:** `raise AirflowFailException("why")`. Nothing to do: `AirflowSkipException`.
- **SQL:** `%s` placeholders, values passed separately; never paste inputs into SQL (example 6).
- **Scripts that live elsewhere:** full paths set once at the top of the job file (example 8).
- **Reusable bash script:** one folder in `template_searchpath`, `bash_command="name.sh"`, different `env={...}` per task (example 8).
- **Optional step:** checkbox + `@task.short_circuit` (example 8), or exit code 99 to skip one bash step (example 11).
- **A step after a skipped step:** `trigger_rule="none_failed"` (example 11) or `"none_failed_min_one_success"` (example 4).
- **Don't pass DataFrames between tasks:** build and load in one task; the next reads from the DB or a file (example 9).
- **Safe reruns:** delete the batch's rows, then insert, in one transaction (examples 8, 9).
- **One run at a time:** `max_active_runs=1` (example 7).
