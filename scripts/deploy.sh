#!/usr/bin/env bash
set -euo pipefail

# Compatibilidad rapida: la creacion de instancias se maneja desde src/levantar_kafka.py.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKERS="${1:-2}"

python3 "$ROOT_DIR/src/levantar_kafka.py" --start_nodes "$WORKERS"
