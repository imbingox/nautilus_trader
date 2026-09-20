#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <source-ref> <output-directory>" >&2
  exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "${script_dir}/../.." && pwd -P)"
source_ref=$1
output_dir=$2
uv_bin="${PAPI_RELEASE_UV:-uv}"

cd "$repo_root"
source_sha="$(git rev-parse --verify "${source_ref}^{commit}")"
head_sha="$(git rev-parse HEAD)"
if [[ "$source_sha" != "$head_sha" ]]; then
  echo "Error: source ref ${source_ref} resolves to ${source_sha}, HEAD is ${head_sha}" >&2
  exit 1
fi
if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
  echo "Error: formal PAPI wheels require a clean source checkout" >&2
  exit 1
fi
if [[ "$(uname -s)" != Linux || "$(uname -m)" != x86_64 ]]; then
  echo "Error: the formal PAPI wheel target is Linux x86_64" >&2
  exit 1
fi

python_bin="${PAPI_RELEASE_PYTHON:-$($uv_bin python find 3.14)}"
"$python_bin" - << 'PY'
import sys
import sysconfig

if sys.implementation.name != "cpython" or sys.version_info[:2] != (3, 14):
    raise SystemExit("PAPI release builds require CPython 3.14")
if sysconfig.get_config_var("Py_GIL_DISABLED"):
    raise SystemExit("PAPI release builds require the regular GIL-enabled CPython ABI")
PY

if [[ -e "$output_dir" ]]; then
  if [[ ! -d "$output_dir" ]]; then
    echo "Error: output path is not a directory: ${output_dir}" >&2
    exit 1
  fi
  if [[ -n "$(find "$output_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "Error: output directory must be empty: ${output_dir}" >&2
    exit 1
  fi
fi
mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd -P)"

export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${repo_root}/target/papi-release}"
SOURCE_DATE_EPOCH="$(git show -s --format=%ct "$source_sha")"
export SOURCE_DATE_EPOCH
unset PYTHONHOME
unset PYTHONPATH
unset VIRTUAL_ENV

cd "$repo_root/python"
"$uv_bin" run --no-project --no-build \
  --with "maturin==$(bash ../scripts/maturin-version.bash)" \
  -- maturin build \
  --release \
  --locked \
  --compatibility manylinux_2_34 \
  --interpreter "$python_bin" \
  --out "$output_dir"

shopt -s nullglob
wheels=("$output_dir"/*.whl)
shopt -u nullglob
if ((${#wheels[@]} != 1)); then
  echo "Error: expected exactly one wheel, found ${#wheels[@]}" >&2
  exit 1
fi
"$python_bin" "$script_dir/verify_wheel.py" "${wheels[0]}"
echo "Built formal PAPI wheel from ${source_sha}: ${wheels[0]}"
