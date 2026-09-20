#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <testpypi|pypi> <release-manifest>" >&2
  exit 1
fi

registry=$1
manifest=$2
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
uv_bin="${PAPI_RELEASE_UV:-uv}"
max_attempts="${PAPI_INDEX_MAX_ATTEMPTS:-12}"
if [[ ! "$max_attempts" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: PAPI_INDEX_MAX_ATTEMPTS must be a positive integer" >&2
  exit 1
fi

case "$registry" in
  testpypi) index_url="https://test.pypi.org/simple" ;;
  pypi) index_url="https://pypi.org/simple" ;;
  *)
    echo "Error: registry must be testpypi or pypi" >&2
    exit 1
    ;;
esac

IFS=$'\t' read -r distribution version expected_name expected_sha < <(
  "$uv_bin" run --no-project --python 3.14 python - "$manifest" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    manifest = json.load(stream)
artifact = manifest["artifact"]
print(
    manifest["distribution"],
    manifest["version"],
    artifact["filename"],
    artifact["sha256"],
    sep="\t",
)
PY
)

temp_root="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/papi-index.XXXXXX")"
trap 'rm -rf "$temp_root"' EXIT
attempt=1
while ! "$uv_bin" run --no-project --python 3.14 --with "pip==25.2" -- \
  python -m pip download \
    --dest "$temp_root" \
    --index-url "$index_url" \
    --no-cache-dir \
    --no-deps \
    --only-binary :all: \
    --implementation cp \
    --python-version 3.14 \
    --abi cp314 \
    --platform manylinux_2_34_x86_64 \
    "${distribution}==${version}"; do
  if ((attempt >= max_attempts)); then
    echo "Error: ${registry} download failed after ${attempt} attempts" >&2
    exit 1
  fi
  echo "Waiting for ${registry} index propagation (attempt ${attempt}/${max_attempts})" >&2
  sleep 10
  ((attempt += 1))
done

downloaded="${temp_root}/${expected_name}"
if [[ ! -f "$downloaded" ]]; then
  echo "Error: ${registry} returned a different wheel than ${expected_name}" >&2
  exit 1
fi
actual_sha="$(bash "${script_dir}/../ci/publish-wheels-sha256.bash" "$downloaded")"
if [[ "$actual_sha" != "$expected_sha" ]]; then
  echo "Error: ${registry} SHA-256 ${actual_sha} does not match ${expected_sha}" >&2
  exit 1
fi

bash "${script_dir}/verify-wheel.bash" "$temp_root"
echo "Verified ${expected_name} from ${registry} with SHA-256 ${actual_sha}"
