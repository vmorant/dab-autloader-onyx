#!/usr/bin/env bash
# Construye la wheel de odp_framework y la publica, inmutable, en el Volume de
# librerías del entorno. Lo ejecuta el CI tras pasar los tests (y se puede
# lanzar a mano en dev).
#
#   scripts/publish_wheel.sh <env> [profile]
#
# Las versiones no se sobrescriben nunca:
#   - si la versión ya está publicada con el mismo contenido, no hace nada
#     (el CI puede repetirse);
#   - si está publicada con OTRO contenido, falla: hay que subir la versión en
#     pyproject.toml y src/odp_framework/__init__.py, y cambiar framework_wheel
#     en databricks.yml (PR) para cada target.
set -euo pipefail

ENV="${1:?uso: publish_wheel.sh <dev|pre|pro> [profile]}"
PROFILE_ARGS=()
[[ -n "${2:-}" ]] && PROFILE_ARGS=(--profile "$2")

VERSION=$(python3 -c "import tomllib;print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
INIT_VERSION=$(python3 -c "import re;print(re.search(r'__version__ = \"(.+?)\"', open('src/odp_framework/__init__.py').read()).group(1))")
if [[ "$VERSION" != "$INIT_VERSION" ]]; then
  echo "pyproject ($VERSION) y __init__ ($INIT_VERSION) no coinciden" >&2
  exit 1
fi

WHEEL="odp_framework-${VERSION}-py3-none-any.whl"
TARGET="dbfs:/Volumes/odp_platform_${ENV}/libs/wheels/${WHEEL}"

rm -rf dist && uv build --wheel

# Compara el RECORD de las dos wheels (hash de cada fichero): no depende de las
# fechas internas del zip.
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
    echo "La versión ${VERSION} ya está publicada en ${TARGET} con el mismo contenido: nada que hacer."
    exit 0
  fi
  echo "La versión ${VERSION} ya existe en ${TARGET} con OTRO contenido: sube la versión." >&2
  exit 1
fi
databricks fs cp "dist/${WHEEL}" "$TARGET" "${PROFILE_ARGS[@]}"
echo "Publicada ${TARGET}"
