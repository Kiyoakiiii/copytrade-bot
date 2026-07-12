#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: CONFIRM_RESTORE=1 $0 backups/copytrade_YYYYMMDDTHHMMSSZ.dump" >&2
  exit 2
fi

if [[ "${CONFIRM_RESTORE:-}" != "1" ]]; then
  echo "Refusing to restore without CONFIRM_RESTORE=1. Restore replaces database contents." >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUMP_PATH="$1"

if [[ ! -f "${DUMP_PATH}" ]]; then
  echo "Backup file not found: ${DUMP_PATH}" >&2
  exit 2
fi

cd "${ROOT_DIR}"

docker compose up -d postgres redis
docker compose stop backend frontend nginx >/dev/null 2>&1 || true

docker compose cp "${DUMP_PATH}" postgres:/tmp/copytrade_restore.dump >/dev/null
docker compose exec -T postgres pg_restore \
  -U copytrade \
  -d copytrade \
  --clean \
  --if-exists \
  --no-owner \
  /tmp/copytrade_restore.dump
docker compose exec -T postgres rm -f /tmp/copytrade_restore.dump

echo "Database restored from ${DUMP_PATH}"
