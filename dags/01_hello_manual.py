"""
Example 1 · Hello, manual run
=============================
The smallest possible manually triggered job with an input.

What it teaches
  - schedule=None        -> the job never runs on its own; someone clicks Trigger
  - params={...}         -> the inputs; Airflow turns them into a form
  - context["params"]    -> how a task reads the values someone typed in

Try it
  UI:  Trigger -> change "name" -> Trigger
  CLI: airflow dags trigger hello_manual --conf '{"name": "Alex"}'
"""
from datetime import datetime

try:  # Airflow 3
    from airflow.sdk import dag, task, Param
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param


@dag(
    dag_id="hello_manual",
    dag_display_name="Example 1 · Hello, manual run",
    schedule=None,                     # manual runs only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "level-1"],
    params={
        "name": Param("world", type="string", description="Who to greet"),
    },
)
def hello_manual():

    @task
    def say_hello(**context):
        name = context["params"]["name"]
        print(f"Hello, {name}!")

    say_hello()


hello_manual()
