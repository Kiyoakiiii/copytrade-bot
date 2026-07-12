#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../backend"
alembic upgrade head
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

