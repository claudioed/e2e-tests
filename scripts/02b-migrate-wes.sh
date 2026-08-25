#!/usr/bin/env bash
# e2e-tests/scripts/02b-migrate-wes.sh
#
# wes-work-planning's cmd/wes (unlike the other 4 services' cmd/<svc>) does
# NOT run its own migrations on startup — it only opens a pool via
# postgres.Connect. Every other service self-migrates via
# postgres.RunMigrations/Migrate inside its own main(). So this harness
# applies wes-work-planning's *.up.sql files by hand, in filename order,
# using psql inside the postgres:16 container image (no local psql
# dependency required).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

CONTAINER="e2e-postgres-wes"

log "applying wes-work-planning migrations to ${CONTAINER}"
for f in "${WES_REPO}"/migrations/*.up.sql; do
  log "  $(basename "${f}")"
  docker exec -i "${CONTAINER}" psql -U wes -d wes -v ON_ERROR_STOP=1 < "${f}"
done
ok "wes-work-planning migrations applied"
