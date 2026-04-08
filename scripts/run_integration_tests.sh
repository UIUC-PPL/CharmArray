#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHARMNUMERIC_ROOT="${ROOT}/example/charmnumeric"
CORE_BUILD_DIR="${CHARMTYLES_CORE_BUILD_DIR:-${ROOT}/src/charmtyles/core/build}"
CHARMNUMERIC_BUILD_DIR="${CHARMNUMERIC_BUILD_DIR:-${CHARMNUMERIC_ROOT}/src/build}"

if [[ -z "${CHARM_HOME:-}" ]]; then
    echo "CHARM_HOME must be set before running charmnumeric integration tests." >&2
    exit 1
fi

python - <<'PY'
import importlib

for module in ("pyccs",):
    try:
        importlib.import_module(module)
    except ModuleNotFoundError as exc:
        raise SystemExit(f"Missing Python dependency: {module}") from exc
PY

parallelism="${CMAKE_BUILD_PARALLEL_LEVEL:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"

cmake -S "${ROOT}/src/charmtyles/core" -B "${CORE_BUILD_DIR}"
cmake --build "${CORE_BUILD_DIR}" --parallel "${parallelism}"

cmake \
    -S "${CHARMNUMERIC_ROOT}/src" \
    -B "${CHARMNUMERIC_BUILD_DIR}" \
    -DCHARMTYLES_CORE_BUILD_DIR="${CORE_BUILD_DIR}"
cmake --build "${CHARMNUMERIC_BUILD_DIR}" --target server --parallel "${parallelism}"

python -m pip install -e "${ROOT}"
python -m pip install -e "${CHARMNUMERIC_ROOT}[tests]"

export CHARMNUMERIC_SERVER="${CHARMNUMERIC_BUILD_DIR}/server.out"
if [[ -x "${CHARMNUMERIC_BUILD_DIR}/charmrun" ]]; then
    export CHARMNUMERIC_CHARMRUN="${CHARMNUMERIC_BUILD_DIR}/charmrun"
fi

python -m pytest "${CHARMNUMERIC_ROOT}/tests" -m integration -v "$@"
