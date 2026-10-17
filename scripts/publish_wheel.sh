#!/usr/bin/env bash
# Builds the odp_framework wheel and publishes it, immutable, to the environment's
# libraries Volume. The CI runs it after the tests pass (it can also be run by
# hand in dev).
#
#   scripts/publish_wheel.sh <env> [profile]
#
# Versions are never overwritten:
#   - if the version is already published with the same content, it does nothing
#     (the CI can be re-run);
#   - if it is published with DIFFERENT content, it fails: bump the version in
#     pyproject.toml and src/odp_framework/__init__.py, and change framework_wheel
#     in databricks.yml (PR) for each target.
set -euo pipefail

ENV="${1:?usage: publish_wheel.sh <dev|pre|pro> [profile]}"
PROFILE_ARGS=()
[[ -n "${2:-}" ]] && PROFILE_ARGS=(--profile "$2")

VERSION=$(python3 -c "import tomllib;print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
INIT_VERSION=$(python3 -c "import re;print(re.search(r'__version__ = \"(.+?)\"', open('src/odp_framework/__init__.py').read()).group(1))")
if [[ "$VERSION" != "$INIT_VERSION" ]]; then
  echo "pyproject ($VERSION) and __init__ ($INIT_VERSION) do not match" >&2
  exit 1
fi

WHEEL="odp_framework-${VERSION}-py3-none-any.whl"
TARGET="dbfs:/Volumes/odp_platform_${ENV}/libs/wheels/${WHEEL}"

rm -rf dist && uv build --wheel

# Compares the RECORD of both wheels (hash of each file): it does not depend on
# the timestamps inside the zip.
record() {
  python3 - "$1" <<'EOF'
import sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as z:
    name = next(n for n in z.namelist() if n.endswith(".dist-info/RECORD"))
    print("".join(sorted(l for l in z.read(name).decode().splitlines(True) if not l.startswith(name))))
EOF
}

if databricks fs ls "$TARGET" "${PROFILE_ARGS[@]}" >/dev/null 2>&1; then
  TMP=$(mktemp -d)
  trap 'rm -rf "$TMP"' EXIT
  databricks fs cp "$TARGET" "$TMP/$WHEEL" "${PROFILE_ARGS[@]}" >/dev/null
  if [[ "$(record "$TMP/$WHEEL")" == "$(record "dist/$WHEEL")" ]]; then
    echo "Version ${VERSION} is already published at ${TARGET} with the same content: nothing to do."
    exit 0
  fi
  echo "Version ${VERSION} already exists at ${TARGET} with DIFFERENT content: bump the version." >&2
  exit 1
fi
databricks fs cp "dist/${WHEEL}" "$TARGET" "${PROFILE_ARGS[@]}"
echo "Published ${TARGET}"
