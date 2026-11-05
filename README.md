# Airflow examples: manually triggered jobs with inputs

Example Airflow jobs that run when someone clicks **Trigger** and fills in a form,
from a one-line hello to running our existing batch scripts. Plus the teach-me slides.

| Folder | What's in it |
|---|---|
| [`dags/`](dags/README.md) | The example jobs (1–9, 11), setup, expected results, patterns to copy |
| [`gui/`](gui/README.md) | Examples 10–11: starting jobs from a desktop window instead of the web page |
| `jobs/` | Scripts, sample data and SQL the jobs use (can live anywhere on the server) |
| `docs/` | [Slides (PDF)](docs/airflow-teach-me-slides.pdf), [slides (PowerPoint)](docs/airflow-teach-me-slides.pptx), [Mermaid diagram examples](docs/mermaid-examples.md) |

## Try it on your own machine

Mac, Linux or Windows WSL2, with internet and Python 3.10–3.12:

```bash
git clone https://github.com/lllatsyrC/airflow-demo
cd airflow-demo
./run-local.sh install    # once
./run-local.sh            # then open http://localhost:8080
```

Log in as `admin` with the password printed in the terminal
(also in `.airflow/simple_auth_manager_passwords.json.generated`). Ctrl+C stops it.

**New or changed job files** show up without a restart: changes within ~30 s, new files within
~30 s with `run-local.sh` (5 min by default on a server). If a job doesn't appear, check
**Dag import errors** at the top of the Dags page.

## The examples

| # | Job | Shows |
|---|---|---|
| 1 | `hello_manual` | A manual job with one input |
| 2 | `form_inputs_tour` | Every form input type |
| 3 | `sales_report` | Running existing scripts with the inputs (the demo) |
| 4 | `report_branching` | An input picks which steps run |
| 5 | `multi_region_report` | One parallel step per item picked |
| 6 | `mysql_sales_export` | Safe MySQL queries with the inputs |
| 7 | `sales_pipeline` | Everything together: retries, alerts, run record |
| 8 | `batch_run_scripts` | Our batch scripts in order, shared bash runner, optional DB load |
| 9 | `batch_load_trips` | Two-table load, task 2 reads task 1's rows from the DB |
| 10 | `gui/10_start_a_job.py` | A GUI's Run button starts example 8 |
| 11 | `batch_pick_steps` + `gui/11_pick_job_and_steps.py` | A GUI with several options; tick which steps run |

Details and setup: [dags/README.md](dags/README.md) and [gui/README.md](gui/README.md).

## References

- [Airflow: Architecture overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html): what each part of the server does
- [Airflow: Params](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html): every form input type
- [Airflow: Dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html): `.expand()` (examples 5, 7)
- [Airflow: REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html): starting jobs from other programs (examples 10, 11)
- [Airflow: Installing from PyPI](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html): pip with a constraints file, including offline
- [shyarnis/airflow-presentation](https://github.com/shyarnis/airflow-presentation): classroom deck that inspired the workflow and DAG diagrams (its install steps are for Airflow 2.7)
- Harenslak & de Ruiter, *Data Pipelines with Apache Airflow* (Manning), chapters 1–2

Tested on Airflow 3.3.2 and 2.10.5 (Python 3.12) with MariaDB.
