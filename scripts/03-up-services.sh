#!/usr/bin/env bash
# e2e-tests/scripts/03-up-services.sh
#
# Starts all 5 bounded-context HTTP services as background processes
# against Postgres + Kafka, in dependency order:
#   1. facility-layout   — no deps (Open Host Service for the warehouse map)
#   2. inventory-storage — calls facility-layout over HTTP for hazmat/
#                           temperature placement checks (LOCATION_LOOKUP_MODE=http)
#   3. wes-work-planning — calls inventory-storage over HTTP for product
#                           classification (PRODUCT_CLASSIFICATION_MODE=http),
#                           consumes workforce/inventory/fulfillment Kafka topics
#   4. fulfillment-execution — consumes WorkReleased from wes-work-planning's
#                           Kafka topic, calls inventory-storage over HTTP for
#                           DOT hazard segregation, publishes TaskCompleted
#   5. workforce-management — publishes ShiftPlanCommitted to Kafka, which
#                           wes-work-planning's labor-plan-view projects
#
# All five run with EVENT_PUBLISHER=kafka against the shared broker so the
# cross-context event flow (WorkReleased -> Task, TaskCompleted -> WorkUnit
# completion, ShiftPlanCommitted -> labor-plan-view) is exercised for real,
# not just each service in isolation.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

log "starting facility-layout on ${FACILITY_BASE_URL}"
start_service facility "${BIN_DIR}/facility" \
  HTTP_ADDR=":${FACILITY_HTTP_PORT}" \
  DATABASE_URL="${FACILITY_DB_URL}" \
  MIGRATIONS_PATH="${FACILITY_REPO}/migrations" \
  LOG_LEVEL=info
wait_for_http "${FACILITY_BASE_URL}/healthz"

log "starting inventory-storage on ${INVENTORY_BASE_URL}"
start_service inventory "${BIN_DIR}/inventory" \
  HTTP_ADDR=":${INVENTORY_HTTP_PORT}" \
  DATABASE_URL="${INVENTORY_DB_URL}" \
  MIGRATIONS_PATH="${INVENTORY_REPO}/migrations" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  LOCATION_LOOKUP_MODE=http \
  FACILITY_LAYOUT_BASE_URL="${FACILITY_BASE_URL}" \
  LOG_LEVEL=info
wait_for_http "${INVENTORY_BASE_URL}/healthz"

log "starting wes-work-planning on ${WES_BASE_URL}"
start_service wes "${BIN_DIR}/wes" \
  HTTP_ADDR=":${WES_HTTP_PORT}" \
  DATABASE_URL="${WES_DB_URL}" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  PRODUCT_CLASSIFICATION_MODE=http \
  INVENTORY_STORAGE_BASE_URL="${INVENTORY_BASE_URL}" \
  LOG_LEVEL=info
wait_for_http "${WES_BASE_URL}/healthz"

log "starting fulfillment-execution on ${FULFILLMENT_BASE_URL}"
# fulfillment-execution's main() hardcodes postgres.Migrate(databaseURL,
# "migrations") — a relative path, unlike the other services which read
# MIGRATIONS_PATH from the environment — so it must be launched with its
# CWD set to the repo root for that relative path to resolve.
start_service_in execution "${FULFILLMENT_REPO}" "${BIN_DIR}/execution" \
  HTTP_ADDR=":${FULFILLMENT_HTTP_PORT}" \
  DATABASE_URL="${FULFILLMENT_DB_URL}" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  PRODUCT_CLASSIFICATION_MODE=http \
  INVENTORY_STORAGE_BASE_URL="${INVENTORY_BASE_URL}" \
  LOG_LEVEL=info
wait_for_http "${FULFILLMENT_BASE_URL}/healthz"

log "starting workforce-management on ${WORKFORCE_BASE_URL}"
start_service workforce "${BIN_DIR}/workforce" \
  HTTP_ADDR=":${WORKFORCE_HTTP_PORT}" \
  DATABASE_URL="${WORKFORCE_DB_URL}" \
  MIGRATIONS_PATH="${WORKFORCE_REPO}/migrations" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  LOG_LEVEL=info
wait_for_http "${WORKFORCE_BASE_URL}/healthz"

log "all 5 services up and healthy"
printf '  %-24s %s\n' facility-layout        "${FACILITY_BASE_URL}"
printf '  %-24s %s\n' inventory-storage      "${INVENTORY_BASE_URL}"
printf '  %-24s %s\n' wes-work-planning      "${WES_BASE_URL}"
printf '  %-24s %s\n' fulfillment-execution  "${FULFILLMENT_BASE_URL}"
printf '  %-24s %s\n' workforce-management   "${WORKFORCE_BASE_URL}"

# --- MCP servers (cmd/mcp), one per context, pointed at the SAME
# Postgres each HTTP service above just started against — so a fact an
# HTTP call writes (e.g. a shift plan, a work pool) is immediately
# visible to warehouse-ops-agent's MCP-tool reads. Every one of these
# only needs its own MCP_READ_KEY: none of the 5 contexts' T1 outbound
# clients call a write tool. -----------------------------------------

log "starting facility-layout MCP server on :${FACILITY_MCP_PORT}"
start_service facility-mcp "${BIN_DIR}/facility-mcp" \
  MCP_ADDR=":${FACILITY_MCP_PORT}" \
  DATABASE_URL="${FACILITY_DB_URL}" \
  MIGRATIONS_PATH="${FACILITY_REPO}/migrations" \
  MCP_READ_KEY="${FACILITY_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${FACILITY_MCP_PORT}"

log "starting inventory-storage MCP server on :${INVENTORY_MCP_PORT}"
start_service inventory-mcp "${BIN_DIR}/inventory-mcp" \
  MCP_ADDR=":${INVENTORY_MCP_PORT}" \
  DATABASE_URL="${INVENTORY_DB_URL}" \
  MIGRATIONS_PATH="${INVENTORY_REPO}/migrations" \
  MCP_READ_KEY="${INVENTORY_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${INVENTORY_MCP_PORT}"

log "starting wes-work-planning MCP server on :${WES_MCP_PORT}"
start_service wes-mcp "${BIN_DIR}/wes-mcp" \
  MCP_ADDR=":${WES_MCP_PORT}" \
  DATABASE_URL="${WES_DB_URL}" \
  MCP_READ_KEY="${WES_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${WES_MCP_PORT}"

log "starting fulfillment-execution MCP server on :${FULFILLMENT_MCP_PORT}"
# Same relative-migrations-path quirk as cmd/execution (see
# 03-up-services.sh above): must launch with CWD set to the repo root.
start_service_in execution-mcp "${FULFILLMENT_REPO}" "${BIN_DIR}/execution-mcp" \
  MCP_ADDR=":${FULFILLMENT_MCP_PORT}" \
  DATABASE_URL="${FULFILLMENT_DB_URL}" \
  MCP_READ_KEY="${FULFILLMENT_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${FULFILLMENT_MCP_PORT}"

log "starting workforce-management MCP server on :${WORKFORCE_MCP_PORT}"
start_service workforce-mcp "${BIN_DIR}/workforce-mcp" \
  MCP_ADDR=":${WORKFORCE_MCP_PORT}" \
  DATABASE_URL="${WORKFORCE_DB_URL}" \
  MIGRATIONS_PATH="${WORKFORCE_REPO}/migrations" \
  MCP_READ_KEY="${WORKFORCE_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${WORKFORCE_MCP_PORT}"

log "all 5 MCP servers up"

# --- warehouse-ops-agent (T5): the agentic analyze/act layer, wired to
# the 5 MCP servers just started above as its only upstream dependency. --

log "starting warehouse-ops-agent on ${OPS_AGENT_BASE_URL}"
start_service ops-agent "${BIN_DIR}/ops-agent" \
  AGENT_ADDR=":${OPS_AGENT_HTTP_PORT}" \
  WES_WORK_PLANNING_MCP_ENDPOINT="${WES_MCP_URL}" \
  WES_WORK_PLANNING_MCP_READ_KEY="${WES_MCP_READ_KEY}" \
  FULFILLMENT_EXECUTION_MCP_ENDPOINT="${FULFILLMENT_MCP_URL}" \
  FULFILLMENT_EXECUTION_MCP_READ_KEY="${FULFILLMENT_MCP_READ_KEY}" \
  INVENTORY_STORAGE_MCP_ENDPOINT="${INVENTORY_MCP_URL}" \
  INVENTORY_STORAGE_MCP_READ_KEY="${INVENTORY_MCP_READ_KEY}" \
  WORKFORCE_MANAGEMENT_MCP_ENDPOINT="${WORKFORCE_MCP_URL}" \
  WORKFORCE_MANAGEMENT_MCP_READ_KEY="${WORKFORCE_MCP_READ_KEY}" \
  FACILITY_LAYOUT_MCP_ENDPOINT="${FACILITY_MCP_URL}" \
  FACILITY_LAYOUT_MCP_READ_KEY="${FACILITY_MCP_READ_KEY}" \
  MCP_READ_KEY="${OPS_AGENT_MCP_READ_KEY}" \
  DAILY_BRIEF_PATH_TARGETS="${OPS_AGENT_PATH_TARGETS}" \
  LOG_LEVEL=info
wait_for_http "${OPS_AGENT_BASE_URL}/healthz"

log "warehouse-ops-agent up"
printf '  %-24s %s\n' warehouse-ops-agent "${OPS_AGENT_BASE_URL}"
