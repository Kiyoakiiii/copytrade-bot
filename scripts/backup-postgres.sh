#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_DIR="${ROOT_DIR}/backups"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${BACKUP_DIR}/copytrade_${STAMP}.dump"

mkdir -p "${BACKUP_DIR}"
cd "${ROOT_DIR}"

docker compose exec -T postgres pg_dump \
  -U copytrade \
  -d copytrade \
  --format=custom \
  --no-owner \
  --file=/tmp/copytrade.dump

docker compose cp postgres:/tmp/copytrade.dump "${OUT}" >/dev/null
docker compose exec -T postgres rm -f /tmp/copytrade.dump

chmod 600 "${OUT}"
echo "Wrote ${OUT}"
