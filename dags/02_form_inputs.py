"""
Example 2 · Every kind of form input
====================================
A tour of the input types the Trigger form can show. Copy the ones you need.

What it teaches
  - Text, number, checkbox, dropdown, multi-select, date picker,
    multi-line text and JSON inputs
  - Required fields, limits and patterns (Airflow refuses bad values
    before the run starts)
  - title / description to label fields, section to group them

Try it
  UI: Trigger, fill in the required "Run label" and "Project code", and
      try typing a bad value (e.g. row limit 0) to see the validation.
"""
from datetime import datetime

try:  # Airflow 3
    from airflow.sdk import dag, task, Param
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param

REGIONS = ["East", "West", "North", "South"]


@dag(
    dag_id="form_inputs_tour",
    dag_display_name="Example 2 · Every kind of form input",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "level-1"],
    params={
        # ---- Basics -------------------------------------------------------
        # No default -> required: the form will not submit until it is filled in.
        "run_label": Param(
            type="string",
            minLength=3,
            title="Run label",
            description="Required. A short name for this run (3+ characters).",
            section="Basics",
        ),
        # pattern = a regular expression the value must match.
        "project_code": Param(
            type="string",
            pattern="^[A-Z]{2}-[0-9]{4}$",
            title="Project code",
            description="Required. Format AB-1234.",
            section="Basics",
        ),
        # format="multiline" -> a larger text box.
        "notes": Param(
            "",
            type="string",
            format="multiline",
            title="Notes",
            description="Optional free text, saved with the run.",
            section="Basics",
        ),

        # ---- Filters ------------------------------------------------------
        # enum -> dropdown. values_display -> friendlier labels in the form.
        "region": Param(
            "East",
            type="string",
            enum=REGIONS,
            values_display={"East": "East coast", "West": "West coast",
                            "North": "North", "South": "South"},
            title="Main region",
            section="Filters",
        ),
        # array + examples -> multi-select.
        "regions": Param(
            ["East", "West"],
            type="array",
            examples=REGIONS,
            items={"type": "string", "enum": REGIONS},
            minItems=1,
            title="Regions to include",
            description="Pick one or more.",
            section="Filters",
        ),
        # format="date" -> date picker. ["null", "string"] makes it optional.
        "report_date": Param(
            None,
            type=["null", "string"],
            format="date",
            title="Report date",
            description="Optional. Leave empty for 'all dates'.",
            section="Filters",
        ),
        # integer with limits -> number box, validated.
        "row_limit": Param(
            1000,
            type="integer",
            minimum=1,
            maximum=100000,
            title="Row limit",
            section="Filters",
        ),
        # number -> decimals allowed.
        "threshold": Param(
            0.5,
            type="number",
            minimum=0,
            maximum=1,
            title="Threshold (0 to 1)",
            section="Filters",
        ),

        # ---- Advanced -----------------------------------------------------
        # boolean -> checkbox / toggle.
        "dry_run": Param(
            True,
            type="boolean",
            title="Dry run",
            description="Only print what would happen.",
            section="Advanced",
        ),
        # object -> JSON editor, for power users.
        "extra_options": Param(
            {"retries": 1, "verbose": False},
            type="object",
            title="Extra options (JSON)",
            section="Advanced",
        ),
    },
)
def form_inputs_tour():

    @task
    def show_inputs(**context):
        print("Values received for this run:")
        for name, value in context["params"].items():
            print(f"  {name:<14} = {value!r:<30} ({type(value).__name__})")

    show_inputs()


form_inputs_tour()
