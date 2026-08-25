####################################################################
# e2e-tests/.env  (sourced by every scripts/*.sh — edit ports here only)
####################################################################

# ---- repo layout -------------------------------------------------
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOS_ROOT="$(cd "${WORKSPACE_ROOT}/.." && pwd)"

FACILITY_REPO="${REPOS_ROOT}/facility-layout"
INVENTORY_REPO="${REPOS_ROOT}/inventory-storage"
WES_REPO="${REPOS_ROOT}/wes-work-planning"
FULFILLMENT_REPO="${REPOS_ROOT}/fulfillment-execution"
WORKFORCE_REPO="${REPOS_ROOT}/workforce-management"
OPS_AGENT_REPO="${REPOS_ROOT}/warehouse-ops-agent"

BIN_DIR="${WORKSPACE_ROOT}/bin"
LOG_DIR="${WORKSPACE_ROOT}/logs"
RUN_DIR="${WORKSPACE_ROOT}/run"

# ---- HTTP ports (offset from each repo's own default :8080 so all
#      five can run side by side on one machine) -------------------
FACILITY_HTTP_PORT=8081
INVENTORY_HTTP_PORT=8082
WES_HTTP_PORT=8083
FULFILLMENT_HTTP_PORT=8084
WORKFORCE_HTTP_PORT=8085

FACILITY_BASE_URL="http://localhost:${FACILITY_HTTP_PORT}"
INVENTORY_BASE_URL="http://localhost:${INVENTORY_HTTP_PORT}"
WES_BASE_URL="http://localhost:${WES_HTTP_PORT}"
FULFILLMENT_BASE_URL="http://localhost:${FULFILLMENT_HTTP_PORT}"
WORKFORCE_BASE_URL="http://localhost:${WORKFORCE_HTTP_PORT}"

# ---- MCP ports (each context's Streamable-HTTP MCP server, cmd/mcp,
#      alongside its HTTP service above) ----------------------------
FACILITY_MCP_PORT=8091
INVENTORY_MCP_PORT=8092
WES_MCP_PORT=8093
FULFILLMENT_MCP_PORT=8094
WORKFORCE_MCP_PORT=8095

FACILITY_MCP_URL="http://localhost:${FACILITY_MCP_PORT}/mcp"
INVENTORY_MCP_URL="http://localhost:${INVENTORY_MCP_PORT}/mcp"
WES_MCP_URL="http://localhost:${WES_MCP_PORT}/mcp"
FULFILLMENT_MCP_URL="http://localhost:${FULFILLMENT_MCP_PORT}/mcp"
WORKFORCE_MCP_URL="http://localhost:${WORKFORCE_MCP_PORT}/mcp"

# Fixed test-only bearer read keys, one per context's own MCP server —
# same static-bearer-key scheme every context uses in prod (ADR-0008),
# just a throwaway value for this local harness.
FACILITY_MCP_READ_KEY="e2e-facility-mcp-read-key"
INVENTORY_MCP_READ_KEY="e2e-inventory-mcp-read-key"
WES_MCP_READ_KEY="e2e-wes-mcp-read-key"
FULFILLMENT_MCP_READ_KEY="e2e-fulfillment-mcp-read-key"
WORKFORCE_MCP_READ_KEY="e2e-workforce-mcp-read-key"

# ---- warehouse-ops-agent (the agentic decision-support layer, T5) --
OPS_AGENT_HTTP_PORT=8096
OPS_AGENT_BASE_URL="http://localhost:${OPS_AGENT_HTTP_PORT}"
OPS_AGENT_MCP_READ_KEY="e2e-ops-agent-mcp-read-key"

# DAILY_BRIEF_PATH_TARGETS override: points the E3 daily-brief synthesis at
# a dedicated T5 process path ("pick-t5-imbalance", building "wh1", shift
# "shift-t5") instead of ops-agent's built-in default ("pick-zone-a",
# "wh1"/"shift-1" — the same identifiers the bootstrap.feature scenario
# already seeds). Using a disjoint path keeps the T5 exception-decisioning
# scenario deterministic and independent of bootstrap.feature's execution
# order/state, per the T5 card's "deterministic seeding" guardrail.
OPS_AGENT_PATH_TARGETS='[{"siteCode":"WH1","pathId":"pick-t5-imbalance","processPath":"PICK","buildingId":"wh1","shiftId":"shift-t5"}]'

# ---- Postgres (docker-compose.yml in this directory) --------------
FACILITY_DB_URL="postgres://facility:facility@localhost:5441/facility?sslmode=disable"
INVENTORY_DB_URL="postgres://inventory:inventory@localhost:5442/inventory?sslmode=disable"
WES_DB_URL="postgres://wes:wes@localhost:5443/wes?sslmode=disable"
FULFILLMENT_DB_URL="postgres://fulfillment:fulfillment@localhost:5444/fulfillment_execution?sslmode=disable"
WORKFORCE_DB_URL="postgres://workforce:workforce@localhost:5445/workforce?sslmode=disable"

# ---- Kafka: shared broker from ~/warehouse-systems/docker-compose.kafka.yml
KAFKA_BROKERS="localhost:9092"
KAFKA_COMPOSE_FILE="${REPOS_ROOT}/docker-compose.kafka.yml"
KAFKA_CONTAINER_NAME="warehouse-kafka"

# ---- misc -----------------------------------------------------------
HEALTH_TIMEOUT_SECS=60
