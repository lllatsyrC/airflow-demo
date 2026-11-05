# Shared runner for team Python scripts that use one set of flags:
#   --input FILE --batch-id ID [--output FILE] [--db-config FILE]
#
# Airflow finds this file through the job's template_searchpath, so each task only says
#   bash_command="run_python_step.sh"
# and gives its own inputs as environment variables:
#   STEP_NAME    label for the log
#   SCRIPT       full path to the Python script (it can live anywhere on the server)
#   PYTHON_BIN   Python to run it with, e.g. the team's own environment (default: python3)
#   IN_FILE      file to read: the batch file, or the previous step's output
#   BATCH_ID     batch label
#   OUT_FILE     optional: file the script writes
#   DB_CONFIG    optional: MySQL option file the script logs in with
#
# The script runs from its own folder, so scripts that use relative paths keep working.
set -euo pipefail

echo "Step:    ${STEP_NAME}"
echo "Script:  ${SCRIPT}"
echo "Input:   ${IN_FILE}"
echo "Batch:   ${BATCH_ID}"

[[ -f "${SCRIPT}" ]]  || { echo "ERROR: script not found: ${SCRIPT}"; exit 1; }
[[ -f "${IN_FILE}" ]] || { echo "ERROR: input file not found: ${IN_FILE}"; exit 1; }

args=(--input "${IN_FILE}" --batch-id "${BATCH_ID}")
if [[ -n "${OUT_FILE:-}" ]]; then
  mkdir -p "$(dirname "${OUT_FILE}")"
  args+=(--output "${OUT_FILE}")
fi
if [[ -n "${DB_CONFIG:-}" ]]; then
  args+=(--db-config "${DB_CONFIG}")
fi

cd "$(dirname "${SCRIPT}")"
echo "Running: ${PYTHON_BIN:-python3} $(basename "${SCRIPT}") ${args[*]}"
"${PYTHON_BIN:-python3}" "${SCRIPT}" "${args[@]}"

if [[ -n "${OUT_FILE:-}" ]]; then
  [[ -s "${OUT_FILE}" ]] || { echo "ERROR: script finished but wrote nothing to ${OUT_FILE}"; exit 1; }
  echo "Output:  ${OUT_FILE}"
fi
