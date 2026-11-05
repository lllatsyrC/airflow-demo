"""
Small helper for starting and following Airflow jobs from another program (examples 10 and 11).

Three calls to Airflow 3's REST API, using only Python's standard library:
    token  = login()
    run_id = start_run(token, "batch_run_scripts", {"batch_dir": "..."})
    state, steps = run_status(token, "batch_run_scripts", run_id)

Settings come from environment variables:
    AIRFLOW_URL       default http://localhost:8080
    AIRFLOW_USER      default admin
    AIRFLOW_PASSWORD  default: read from .airflow/simple_auth_manager_passwords.json.generated
                      (the file ./run-local.sh's Airflow writes)

Airflow 2: paths start with /api/v1 and there is no /auth/token step (send the user and
password with each request instead).
"""
import json
import os
import urllib.error
import urllib.parse
import urllib.request

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080").rstrip("/")
FINISHED = {"success", "failed"}


def _password():
    if os.environ.get("AIRFLOW_PASSWORD"):
        return os.environ["AIRFLOW_PASSWORD"]
    path = os.path.join(REPO, ".airflow", "simple_auth_manager_passwords.json.generated")
    with open(path) as f:
        return json.load(f)[os.environ.get("AIRFLOW_USER", "admin")]


def _call(method, path, body=None, token=None):
    req = urllib.request.Request(
        AIRFLOW_URL + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json",
                 **({"Authorization": f"Bearer {token}"} if token else {})})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Airflow said {e.code} for {path}: {e.read().decode()[:300]}") from None


def login():
    """Swap a user name and password for a token."""
    user = os.environ.get("AIRFLOW_USER", "admin")
    return _call("POST", "/auth/token", {"username": user, "password": _password()})["access_token"]


def start_run(token, dag_id, inputs):
    """Same as filling in the job's Trigger form and clicking Trigger. Returns the run id."""
    run = _call("POST", f"/api/v2/dags/{dag_id}/dagRuns", {"logical_date": None, "conf": inputs}, token)
    return run["dag_run_id"]


def run_status(token, dag_id, run_id):
    """Overall state ('queued', 'running', 'success', 'failed') and each step's state."""
    rid = urllib.parse.quote(run_id, safe="")
    run = _call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{rid}", token=token)
    tis = _call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{rid}/taskInstances", token=token)
    # steps that have started, in the order they started, then the ones still waiting
    ordered = sorted(tis["task_instances"], key=lambda t: (t["start_date"] or "~", t["task_id"]))
    return run["state"], {t["task_id"]: t["state"] or "waiting" for t in ordered}


def run_link(dag_id, run_id):
    """The run's page in the Airflow web UI."""
    return f"{AIRFLOW_URL}/dags/{dag_id}/runs/{urllib.parse.quote(run_id, safe='')}"
