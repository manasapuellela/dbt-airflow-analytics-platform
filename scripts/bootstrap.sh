#!/usr/bin/env bash
set -euo pipefail

cp -n .env.example .env || true
cp -n dbt/profiles.yml.example dbt/profiles.yml || true

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r src/ingest/requirements.txt
pip install dbt-core==1.7.7 dbt-postgres==1.7.7 great-expectations==0.18.8 ruff==0.3.2 pytest==8.0.0

echo "Bootstrap complete. Activate your venv with: source .venv/bin/activate"
