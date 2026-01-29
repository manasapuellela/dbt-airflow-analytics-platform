#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "$ROOT_DIR/.env.example" ]; then
  cp -n "$ROOT_DIR/.env.example" "$ROOT_DIR/.env" || true
fi

if [ -f "$ROOT_DIR/dbt/profiles.yml.example" ]; then
  cp -n "$ROOT_DIR/dbt/profiles.yml.example" "$ROOT_DIR/dbt/profiles.yml" || true
fi

python3 -m venv "$ROOT_DIR/.venv"
source "$ROOT_DIR/.venv/bin/activate"
pip install --upgrade pip
pip install -r "$ROOT_DIR/src/ingest/requirements.txt"
pip install dbt-core==1.7.7 dbt-postgres==1.7.7 great-expectations==0.18.8 ruff==0.3.2 pytest==8.0.0

echo "Bootstrap complete. Activate your venv with: source .venv/bin/activate"
