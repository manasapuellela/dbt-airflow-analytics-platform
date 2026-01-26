#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
else
  echo "No .env file found. Copy .env.example to .env first." >&2
  exit 1
fi
