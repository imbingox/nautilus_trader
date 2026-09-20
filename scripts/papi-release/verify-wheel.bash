#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <wheel-directory>" >&2
  exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "${script_dir}/../.." && pwd -P)"
wheel_dir="$(cd "$1" && pwd -P)"
uv_bin="${PAPI_RELEASE_UV:-uv}"
python_bin="${PAPI_RELEASE_PYTHON:-$($uv_bin python find 3.14)}"
report_path="${wheel_dir}/wheel-verification.json"

shopt -s nullglob
wheels=("$wheel_dir"/*.whl)
shopt -u nullglob
if ((${#wheels[@]} != 1)); then
  echo "Error: expected exactly one wheel in ${wheel_dir}, found ${#wheels[@]}" >&2
  exit 1
fi
wheel="${wheels[0]}"

"$python_bin" "$script_dir/verify_wheel.py" "$wheel" --report "$report_path"

if [[ "$(uname -s)" != Linux || "$(uname -m)" != x86_64 ]]; then
  echo "Error: formal native dependency validation requires Linux x86_64" >&2
  exit 1
fi

"$uv_bin" run --no-project --no-build --with "auditwheel==6.4.2" -- \
  auditwheel show "$wheel"
"$uv_bin" run --no-project --no-build --with "twine==6.2.0" -- \
  twine check --strict "$wheel"

temp_root="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/papi-wheel.XXXXXX")"
trap 'rm -rf "$temp_root"' EXIT
environment="${temp_root}/environment"
"$uv_bin" venv --python "$python_bin" "$environment"
wheel_python="${environment}/bin/python"
"$uv_bin" pip install --python "$wheel_python" --no-deps --only-binary :all: "$wheel"
"$uv_bin" pip check --python "$wheel_python"
(
  cd "$temp_root"
  "$wheel_python" -I \
    "$repo_root/python/tests/integration/binance_papi_wheel_smoke.py" enabled
  "$wheel_python" -I - <<'PY'
import importlib.metadata

try:
    importlib.metadata.distribution("nautilus-trader")
except importlib.metadata.PackageNotFoundError:
    pass
else:
    raise SystemExit("Official nautilus-trader distribution is installed beside the PAPI fork")
PY
)

"$python_bin" - "$report_path" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
report = json.loads(path.read_text(encoding="utf-8"))
report.update(
    {
        "auditwheel": "passed",
        "isolated_install": "passed",
        "pip_check": "passed",
        "twine_check": "passed",
        "wheel_smoke": "passed",
        "complete": True,
    },
)
path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

echo "Completed formal PAPI wheel verification: ${report_path}"
