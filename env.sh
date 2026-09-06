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
ORDER_REPO="${REPOS_ROOT}/order-management"
# process-path-management (8th bounded context — Generic Subdomain owning
# the fleet's declared process-path catalogue, replacing the static
# config/process-paths/*.yaml file the other five services used to boot
# from — see warehouse-infra's ADR for the Kafka-propagation redesign).
PROCESS_PATH_REPO="${REPOS_ROOT}/process-path-management"
# labor-performance (7th bounded context — engineered labor standards /
# performance scoring, a pure Kafka consumer of fulfillment-execution's
# TaskCompleted event; no HTTP dependency on any other context, so it
# starts after fulfillment-execution purely so its consumer has a real
# topic to subscribe to from the start, not because of a synchronous
# call).
LABOR_REPO="${REPOS_ROOT}/labor-performance"

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
# order-management (6th bounded context, choreographed-release redesign —
# see CLAUDE.md's "Cross-service integration" section) — next free slot
# after workforce-management's :8085.
ORDER_HTTP_PORT=8086
# process-path-management (8th bounded context) — next free slot after
# order-management's :8086.
PROCESS_PATH_HTTP_PORT=8087
# labor-performance (7th bounded context) — next free slot after
# process-path-management's :8087.
LABOR_HTTP_PORT=8088

FACILITY_BASE_URL="http://localhost:${FACILITY_HTTP_PORT}"
INVENTORY_BASE_URL="http://localhost:${INVENTORY_HTTP_PORT}"
WES_BASE_URL="http://localhost:${WES_HTTP_PORT}"
FULFILLMENT_BASE_URL="http://localhost:${FULFILLMENT_HTTP_PORT}"
WORKFORCE_BASE_URL="http://localhost:${WORKFORCE_HTTP_PORT}"
ORDER_BASE_URL="http://localhost:${ORDER_HTTP_PORT}"
PROCESS_PATH_BASE_URL="http://localhost:${PROCESS_PATH_HTTP_PORT}"
LABOR_BASE_URL="http://localhost:${LABOR_HTTP_PORT}"

# ---- MCP ports (each context's Streamable-HTTP MCP server, cmd/mcp,
#      alongside its HTTP service above) ----------------------------
FACILITY_MCP_PORT=8091
INVENTORY_MCP_PORT=8092
WES_MCP_PORT=8093
FULFILLMENT_MCP_PORT=8094
WORKFORCE_MCP_PORT=8095
# labor-performance (7th bounded context) -- placed after
# OPS_AGENT_HTTP_PORT (8096) below rather than immediately following
# WORKFORCE_MCP_PORT, since 8096 is already claimed by ops-agent's own
# HTTP port. Registered here for harness parity with the other 5
# contexts' MCP servers; NOT yet wired into warehouse-ops-agent's config
# (no T5 use case consumes it today -- see 03-up-services.sh's own note
# on the labor-mcp block for the full rationale).
LABOR_MCP_PORT=8097

FACILITY_MCP_URL="http://localhost:${FACILITY_MCP_PORT}/mcp"
INVENTORY_MCP_URL="http://localhost:${INVENTORY_MCP_PORT}/mcp"
WES_MCP_URL="http://localhost:${WES_MCP_PORT}/mcp"
FULFILLMENT_MCP_URL="http://localhost:${FULFILLMENT_MCP_PORT}/mcp"
WORKFORCE_MCP_URL="http://localhost:${WORKFORCE_MCP_PORT}/mcp"
LABOR_MCP_URL="http://localhost:${LABOR_MCP_PORT}/mcp"

# Fixed test-only bearer read keys, one per context's own MCP server —
# same static-bearer-key scheme every context uses in prod (ADR-0008),
# just a throwaway value for this local harness.
FACILITY_MCP_READ_KEY="e2e-facility-mcp-read-key"
INVENTORY_MCP_READ_KEY="e2e-inventory-mcp-read-key"
WES_MCP_READ_KEY="e2e-wes-mcp-read-key"
FULFILLMENT_MCP_READ_KEY="e2e-fulfillment-mcp-read-key"
WORKFORCE_MCP_READ_KEY="e2e-workforce-mcp-read-key"
LABOR_MCP_READ_KEY="e2e-labor-mcp-read-key"

# ---- warehouse-ops-agent (the agentic decision-support layer, T5) --
OPS_AGENT_HTTP_PORT=8096
OPS_AGENT_BASE_URL="http://localhost:${OPS_AGENT_HTTP_PORT}"
OPS_AGENT_MCP_READ_KEY="e2e-ops-agent-mcp-read-key"

# ---- Analytics *-reports ports (each context's separate read-only reports
#      binary -- cmd/<svc>-reports, backed by its own analytical Postgres,
#      NOT the OLTP HTTP ports above) -- feeds warehouse-ops-agent's
#      console-bff WMS/WES dashboard fan-out (GET /console/reports/wms and
#      /wes; see internal/config/config.go's *ReportsRESTURL fields).
#
#      New port assignment, not an existing convention: every *-reports
#      binary defaults to the SAME HTTP_ADDR=":8092" today (which also
#      collides with INVENTORY_MCP_PORT above), so running more than one
#      locally already needed a per-service override before this harness
#      ever cared about reports ports. This 8101-8107 range mirrors the
#      8081-8086 OLTP ordering above, shifted by +20, clear of the existing
#      8081-8096 OLTP/MCP/agent range this file already occupies.
FACILITY_REPORTS_HTTP_PORT=8101
INVENTORY_REPORTS_HTTP_PORT=8102
WES_REPORTS_HTTP_PORT=8103
FULFILLMENT_REPORTS_HTTP_PORT=8104
WORKFORCE_REPORTS_HTTP_PORT=8105
ORDER_REPORTS_HTTP_PORT=8106
# labor-performance is not (yet) one of this harness's orchestrated OLTP
# services (no LABOR_REPO/LABOR_HTTP_PORT above) -- its reports port is
# still assigned here, in sequence, purely so
# LABOR_PERFORMANCE_REPORTS_REST_URL lines up with warehouse-ops-agent's
# own env var naming if/when this harness starts that service too.
LABOR_REPORTS_HTTP_PORT=8107

FACILITY_REPORTS_BASE_URL="http://localhost:${FACILITY_REPORTS_HTTP_PORT}"
INVENTORY_REPORTS_BASE_URL="http://localhost:${INVENTORY_REPORTS_HTTP_PORT}"
WES_REPORTS_BASE_URL="http://localhost:${WES_REPORTS_HTTP_PORT}"
FULFILLMENT_REPORTS_BASE_URL="http://localhost:${FULFILLMENT_REPORTS_HTTP_PORT}"
WORKFORCE_REPORTS_BASE_URL="http://localhost:${WORKFORCE_REPORTS_HTTP_PORT}"
ORDER_REPORTS_BASE_URL="http://localhost:${ORDER_REPORTS_HTTP_PORT}"
LABOR_REPORTS_BASE_URL="http://localhost:${LABOR_REPORTS_HTTP_PORT}"

# Maps 1:1 onto warehouse-ops-agent's own env var names, so a local run of
# the agent against this harness's services can source this file directly
# rather than re-deriving the mapping by hand.
FACILITY_LAYOUT_REPORTS_REST_URL="${FACILITY_REPORTS_BASE_URL}"
INVENTORY_STORAGE_REPORTS_REST_URL="${INVENTORY_REPORTS_BASE_URL}"
WES_WORK_PLANNING_REPORTS_REST_URL="${WES_REPORTS_BASE_URL}"
FULFILLMENT_EXECUTION_REPORTS_REST_URL="${FULFILLMENT_REPORTS_BASE_URL}"
WORKFORCE_MANAGEMENT_REPORTS_REST_URL="${WORKFORCE_REPORTS_BASE_URL}"
ORDER_MANAGEMENT_REPORTS_REST_URL="${ORDER_REPORTS_BASE_URL}"
LABOR_PERFORMANCE_REPORTS_REST_URL="${LABOR_REPORTS_BASE_URL}"

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
# order-management's own docker-compose.yml defaults to host port 5434 —
# this harness's own e2e-specific offset continues past workforce's :5445
# (avoiding both the 5441-5445 range already in use here AND order-
# management's own :5434 default, per this file's own port-offset
# convention documented in docker-compose.yml's header comment).
ORDER_DB_URL="postgres://order:order@localhost:5446/order?sslmode=disable"
# process-path-management (8th bounded context) — next free slot after
# order-management's :5446.
PROCESS_PATH_DB_URL="postgres://process_path:process_path@localhost:5447/process_path?sslmode=disable"
# labor-performance (7th bounded context) — next free slot after
# process-path-management's :5447.
LABOR_DB_URL="postgres://labor:labor@localhost:5448/labor?sslmode=disable"

# ---- Kafka: single broker platform-wide, owned by the warehouse-infra
#      kind cluster and exposed to the host at localhost:9092 via a
#      Bitnami externalAccess NodePort (warehouse-infra PR #6). This
#      harness does not start its own broker -- see scripts/02-up-infra.sh.
KAFKA_BROKERS="localhost:9092"

# ---- misc -----------------------------------------------------------
HEALTH_TIMEOUT_SECS=60
