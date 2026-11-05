# Airflow examples: manually triggered jobs with inputs

Seven example jobs, each adding one idea to the one before. Every job runs
only when someone clicks **Trigger**, and the form asks for its inputs.

**Slides:** [docs/airflow-teach-me-slides.pdf](docs/airflow-teach-me-slides.pdf), the 29-slide teach-me deck.
It explains the ideas with workflow diagrams: manual steps becoming tasks, what a DAG is (and why loops aren't allowed),
how `>>` draws the arrows, what runs on the server, what happens when you click Trigger, how to read a failed run,
and a diagram of examples 4, 5 and 7.

| # | Job (dag_id) | Adds | Level |
|---|---|---|---|
| 1 | `hello_manual` | A manual job with one input | Start here |
| 2 | `form_inputs_tour` | Every input type: dropdown, multi-select, date picker, checkbox, number limits, required fields, sections | Start here |
| 3 | `sales_report` | Runs our existing scripts with the inputs; steps in order; passes a value between steps (the teach-me demo) | Core |
| 4 | `report_branching` | An input decides which steps run (branching, skipped steps) | Core |
| 5 | `multi_region_report` | One step per region picked in a multi-select, run in parallel, then combined | Intermediate |
| 6 | `mysql_sales_export` | Queries MySQL safely with the inputs; date range; fails fast on bad dates | Intermediate |
| 7 | `sales_pipeline` | All of the above plus retries, failure alerts, a run manifest (who ran it, with what), skip-when-empty, one run at a time, and starting another job | Advanced |

Each file starts with a comment saying what it teaches and how to try it.
In the UI, the jobs are named "Example 1 · …" to "Example 7 · …" and tagged `example`.

```
airflow-demo/
├── dags/                         # put these in Airflow's dags folder
│   ├── 01_hello_manual.py
│   ├── 02_form_inputs.py
│   ├── 03_sales_report.py
│   ├── 04_report_branching.py
│   ├── 05_multi_region_report.py
│   ├── 06_mysql_sales_export.py
│   └── 07_sales_pipeline.py
├── jobs/                         # scripts, data and SQL: anywhere on the server
    ├── scripts/filter_data.py, summarize.py
    ├── data/sales.csv
    └── sql/create_sample_sales.sql
└── docs/airflow-teach-me-slides.pdf
```

## Setup

```bash
export AIRFLOW_HOME=/opt/airflow
export JOBS_DIR=/opt/jobs             # where you put the jobs/ folder
cp dags/*.py $AIRFLOW_HOME/dags/
airflow dags list-import-errors       # should print "No data found"
```
Set `JOBS_DIR` in the environment of the scheduler and workers too (e.g. in
their systemd unit files). Without it, the examples look for `jobs/` next to
the `dags/` folder.

New jobs start **paused**: switch each one on in the UI before triggering.

### Example 6 only: MySQL
```bash
mysql -u <admin user> -p < jobs/sql/create_sample_sales.sql   # own database: airflow_examples
airflow connections add mysql_default --conn-type mysql \
  --conn-host <mysql host> --conn-port 3306 --conn-schema airflow_examples \
  --conn-login <user> --conn-password '<password>'
```
The provider must be installed: `pip install "apache-airflow[mysql]"`. On Linux
this needs the MySQL client headers (`mysql-devel` or `libmysqlclient-dev`) and `gcc`.

## Running them

UI: open the job → **Trigger** → fill in the form → **Trigger**.

CLI, with inputs:
```bash
airflow dags trigger hello_manual        --conf '{"name": "Alex"}'
airflow dags trigger sales_report        --conf '{"region": "East", "min_amount": 100}'
airflow dags trigger report_branching    --conf '{"report_type": "both"}'
airflow dags trigger multi_region_report --conf '{"regions": ["East", "South"], "min_amount": 50}'
airflow dags trigger mysql_sales_export  --conf '{"start_date": "2026-10-02", "end_date": "2026-10-04"}'
airflow dags trigger sales_pipeline      --conf '{"requested_by": "Alex", "regions": ["East", "West"]}'
```
Inputs you leave out keep their defaults. Required inputs (examples 2 and 7)
must be given, or the run is refused.

To try a job once in the terminal without the scheduler, use `airflow dags test <job> -c '<json>'`.

## Expected results on the sample data

| Run | Result |
|---|---|
| `sales_report` East, min 100 | 3 rows, total 610.50 |
| `sales_report` West, min 100 | 1 row, total 400.00 |
| `multi_region_report` East/South/West, min 50 | East 706.25, West 475.00, South 350.00 |
| `mysql_sales_export` 2026-10-02 to 10-04, East + West | 4 rows, total 985.75 |
| `sales_pipeline` East + West, 2026-10-01 to 10-04 | 6 rows, total 1,181.25 |
| `sales_pipeline` min_amount 99999 | Report step skipped, run succeeds |
| `sales_pipeline` To before From | Fails at once, no retries, line added to `failures.log` |

## Patterns to copy into your own jobs

- **Inputs as a form:** `params={...}` with `Param(...)`. See example 2 for every type.
- **Read an input:** `context["params"]["name"]` in Python, `{{ params.name }}` in a command.
- **Required input:** leave out the default: `Param(type="string")`.
- **Stop on bad input without retrying:** `raise AirflowFailException("why")`.
- **Nothing to do is not an error:** `raise AirflowSkipException("why")`.
- **Never paste inputs into SQL text:** use `%s` placeholders and pass the values separately (example 6).
- **Record who ran it:** a required "Your name" input plus a manifest file (example 7).
- **Don't let two runs overwrite each other:** `max_active_runs=1` and a per-run output folder.


## References

| Resource | What it's good for |
|---|---|
| [Airflow docs: Architecture Overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html) | What each part of an Airflow 3 server does (scheduler, DAG processor, API server, metadata database, workers), how tasks pass data, and the default "wait for everything upstream" rule. Basis for the setup and failure slides. |
| [Airflow docs: Params](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html) | Every input type the Trigger form supports. Basis for example 2. |
| [Airflow docs: Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) | `.expand()`, one task copy per item. Basis for examples 5 and 7. |
| [Airflow docs: Installation from PyPI](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html) | Installing with pip and a constraints file, including offline installs. |
| [shyarnis/airflow-presentation](https://github.com/shyarnis/airflow-presentation) | A short classroom deck on Airflow. Its "pipelines as graphs", "why a cycle deadlocks" and "mental model to tasks" slides inspired the workflow and DAG diagrams here. Note: its install steps target Airflow 2.7 and Python 3.8 (`airflow webserver`, `airflow users create`); on Airflow 3 the UI runs as `airflow api-server`. |
| Harenslak, B. and de Ruiter, J., *Data Pipelines with Apache Airflow*, Manning ([book code](https://github.com/BasPH/data-pipelines-with-apache-airflow)) | The book the shyarnis deck draws on. Chapters 1–2 cover the same DAG fundamentals in depth. |

Tested on Airflow 3.3.2 and 2.10.5 (Python 3.12), with MariaDB for example 6.
