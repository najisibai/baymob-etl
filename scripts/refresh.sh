#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# activate venv
source .venv/bin/activate
# incremental pull (since last row in DB), then transform+load are not needed
python - <<'PY'
from etl.extract.sf311_incremental import main as run
run()
PY
