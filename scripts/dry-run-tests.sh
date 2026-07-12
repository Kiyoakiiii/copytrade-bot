#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../backend"
PYTHONPATH="$PWD" python -m pytest -q tests

