#!/usr/bin/env bash
# Run the example jobs and the Airflow web UI on your own machine (Mac, Linux, or WSL2 on Windows).
#
#   First time:   ./run-local.sh install     (needs internet and Python 3.10-3.12)
#   Every time:   ./run-local.sh             then open http://localhost:8080
#
# Log in as "admin". The password is printed when Airflow starts and saved in
# .airflow/simple_auth_manager_passwords.json.generated. Stop with Ctrl+C.
set -euo pipefail
cd "$(dirname "$0")"
AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.3.2}"

if [[ "${1:-}" == "install" ]]; then
  python3 -m venv .venv
  source .venv/bin/activate
  PY=$(python -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
  pip install --upgrade pip
  pip install "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
  # Examples 8 and 9 use pandas; example 8's scripts also use pymysql.
  pip install pandas pymysql \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
  echo "Installed Airflow ${AIRFLOW_VERSION}. Now run: ./run-local.sh"
  exit 0
fi

if [[ ! -d .venv ]]; then
  echo "Airflow isn't installed here yet. Run: ./run-local.sh install"
  exit 1
fi

source .venv/bin/activate
export AIRFLOW_HOME="$PWD/.airflow"               # Airflow's own files stay in this folder
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"     # the example jobs
export AIRFLOW__CORE__LOAD_EXAMPLES=False         # hide Airflow's built-in demo jobs
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
export JOBS_DIR="$PWD/jobs"                       # scripts and sample data

echo "Starting Airflow ${AIRFLOW_VERSION}: open http://localhost:8080 (user: admin, password below)."
exec airflow standalone
