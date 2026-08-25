#!/usr/bin/env bash
# e2e-tests/scripts/02-up-infra.sh
#
# Brings up the shared Kafka broker (if not already running) and this
# harness's five dedicated Postgres instances (docker-compose.yml in this
# directory), then waits for all of them to report healthy.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

command -v docker >/dev/null 2>&1 || die "docker is not on PATH"
docker info >/dev/null 2>&1 || die "the Docker daemon is not reachable — start Docker Desktop first"

if docker ps --format '{{.Names}}' | grep -qx "${KAFKA_CONTAINER_NAME}"; then
  ok "shared Kafka broker '${KAFKA_CONTAINER_NAME}' already running"
else
  log "starting shared Kafka broker via ${KAFKA_COMPOSE_FILE}"
  docker compose -f "${KAFKA_COMPOSE_FILE}" up -d
fi
wait_for_tcp localhost 9092 30

log "starting per-service Postgres instances (docker compose, project warehouse-e2e)"
docker compose -f "${WORKSPACE_ROOT}/docker-compose.yml" up -d

log "waiting for all 6 Postgres instances to report healthy"
deadline=$((SECONDS + 60))
for svc in postgres-facility postgres-inventory postgres-wes postgres-fulfillment postgres-workforce postgres-order; do
  while true; do
    status="$(docker inspect -f '{{.State.Health.Status}}' "e2e-${svc}" 2>/dev/null || echo starting)"
    if [[ "${status}" == "healthy" ]]; then
      ok "${svc} healthy"
      break
    fi
    if [[ "${SECONDS}" -ge "${deadline}" ]]; then
      die "${svc} did not become healthy in time (status=${status})"
    fi
    sleep 1
  done
done

log "infra up: Kafka on :9092, Postgres on :5441-:5446"
