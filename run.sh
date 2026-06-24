#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

export FB_COOKIES="${FB_COOKIES:-$PWD/cookies.json}"
export FB_TRIGGER="${FB_TRIGGER:-bot}"
export FB_ALLOW_THREADS="${FB_ALLOW_THREADS:-}"
export HERMES_BIN="${HERMES_BIN:-hermes}"
export HERMES_TIMEOUT="${HERMES_TIMEOUT:-180}"
export PYTHONUNBUFFERED=1

exec ./venv/bin/python -u bridge.py 2>&1 | tee -a bridge.log
