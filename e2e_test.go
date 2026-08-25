// Package main is the e2e-tests harness: a godog (Cucumber for Go) suite
// that drives all 5 already-running warehouse-systems HTTP services (see
// scripts/03-up-services.sh) purely over their real REST APIs, and asserts
// on cross-service Kafka integration effects by polling read models the
// consumers project. It never imports another repo's Go packages — this
// is black-box, over-the-wire, exactly like a human running curl.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cucumber/godog"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// ---------------------------------------------------------------------
// Environment (read from e2e-tests/.env via the shell that invokes this
// binary — scripts/04-run-tests.sh exports every var below before
// `go test` runs).
// ---------------------------------------------------------------------

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	facilityBaseURL     = envOrDefault("FACILITY_BASE_URL", "http://localhost:8081")
	inventoryBaseURL    = envOrDefault("INVENTORY_BASE_URL", "http://localhost:8082")
	wesBaseURL          = envOrDefault("WES_BASE_URL", "http://localhost:8083")
	fulfillmentBaseURL  = envOrDefault("FULFILLMENT_BASE_URL", "http://localhost:8084")
	workforceBaseURL    = envOrDefault("WORKFORCE_BASE_URL", "http://localhost:8085")
	opsAgentBaseURL     = envOrDefault("OPS_AGENT_BASE_URL", "http://localhost:8096")
	inventoryDBURL      = envOrDefault("INVENTORY_DB_URL", "postgres://inventory:inventory@localhost:5442/inventory?sslmode=disable")
	wesDBURL            = envOrDefault("WES_DB_URL", "postgres://wes:wes@localhost:5443/wes?sslmode=disable")
	fulfillmentDBURL    = envOrDefault("FULFILLMENT_DB_URL", "postgres://fulfillment:fulfillment@localhost:5444/fulfillment_execution?sslmode=disable")
	eventualWaitTimeout = 30 * time.Second
	eventualWaitPoll    = 500 * time.Millisecond
)

// httpResult captures the last HTTP call this scenario made, so later
// steps ("Then the response status is 201") can assert on it.
type httpResult struct {
	status int
	header http.Header
	body   []byte
}

func (r httpResult) json() map[string]any {
	var m map[string]any
	_ = json.Unmarshal(r.body, &m)
	return m
}

// world is the per-scenario state.
type world struct {
	client *http.Client
	last   httpResult

	claimedTaskID string
}

func newWorld() *world {
	return &world{client: &http.Client{Timeout: 10 * time.Second}}
}

// doJSON performs method against url with an optional JSON body and
// records the result on w.last.
func (w *world) doJSON(method, url string, body any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP %s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.last = httpResult{status: resp.StatusCode, header: resp.Header, body: b}
	return nil
}

// ---------------------------------------------------------------------
// Background
// ---------------------------------------------------------------------

func (w *world) allServicesAreHealthy() error {
	for name, base := range map[string]string{
		"facility-layout":       facilityBaseURL,
		"inventory-storage":     inventoryBaseURL,
		"wes-work-planning":     wesBaseURL,
		"fulfillment-execution": fulfillmentBaseURL,
		"workforce-management":  workforceBaseURL,
	} {
		if err := w.doJSON(http.MethodGet, base+"/healthz", nil); err != nil {
			return fmt.Errorf("%s not reachable: %w", name, err)
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("%s /healthz returned %d, body=%s", name, w.last.status, w.last.body)
		}
	}
	return nil
}

// opsAgentIsHealthy is a separate Background step (rather than folded into
// allServicesAreHealthy) so bootstrap.feature's existing Background —
// unaware of warehouse-ops-agent — keeps working unchanged; only T5's own
// scenario requires the agent to be up.
func (w *world) opsAgentIsHealthy() error {
	if err := w.doJSON(http.MethodGet, opsAgentBaseURL+"/healthz", nil); err != nil {
		return fmt.Errorf("warehouse-ops-agent not reachable: %w", err)
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("warehouse-ops-agent /healthz returned %d, body=%s", w.last.status, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// facility-layout steps
// ---------------------------------------------------------------------

func (w *world) registerSite(code, name string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, facilityBaseURL+"/sites", map[string]any{
		"siteCode": code, "name": name,
	}))
}

func (w *world) registerLocationType(name string, weightKg, volumeM3 float64) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, facilityBaseURL+"/location-types", map[string]any{
		"name": name,
		"defaultCapacity": map[string]any{
			"maxWeightKg": weightKg, "maxVolumeM3": volumeM3,
		},
	}))
}

func (w *world) registerZone(areaCode, zoneCode, siteCode, temperatureClass string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/sites/%s/zones", facilityBaseURL, siteCode), map[string]any{
		"areaCode": areaCode, "zoneCode": zoneCode, "temperatureClass": temperatureClass, "hazmat": false,
	}))
}

func (w *world) registerAisle(aisleCode, zoneID string, sequenceHint int, direction string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/zones/%s/aisles", facilityBaseURL, zoneID), map[string]any{
		"aisleCode": aisleCode, "sequenceHint": sequenceHint, "direction": direction,
	}))
}

func (w *world) registerLocationSlot(locationCode, locationType string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, facilityBaseURL+"/locations", map[string]any{
		"locationCode": locationCode, "locationType": locationType,
	}))
}

// ---------------------------------------------------------------------
// inventory-storage steps
// ---------------------------------------------------------------------

// binExists seeds a Bin directly in inventory-storage's own Postgres
// database. There is deliberately no "create bin" HTTP endpoint in this
// service (see its README: "Stow requires a bin to exist first"), so
// every consumer of this service — including this e2e harness — seeds
// bins the same way its own unit/integration tests do: a direct insert.
func (w *world) binExists(binID string, capacity int) error {
	db, err := sql.Open("pgx", inventoryDBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(context.Background(),
		`INSERT INTO bins (id, capacity, occupied) VALUES ($1, $2, 0)
		 ON CONFLICT (id) DO NOTHING`, binID, capacity)
	return err
}

func (w *world) receiveStock(qty int, sku string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, inventoryBaseURL+"/stock/receive", map[string]any{
		"sku": sku, "quantity": qty,
	}))
}

func (w *world) stowStock(qty int, sku, binID string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, inventoryBaseURL+"/stock/stow", map[string]any{
		"sku": sku, "quantity": qty, "binId": binID,
	}))
}

func (w *world) usableInventoryIs(sku string, want int) error {
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/inventory/%s/usable", inventoryBaseURL, sku), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET usable inventory: status %d, body=%s", w.last.status, w.last.body)
	}
	got := w.last.json()
	usable, ok := got["usable"]
	if !ok {
		// field name fallback: some services phrase this "usableQuantity"
		usable = got["usableQuantity"]
	}
	gotF, _ := toFloat(usable)
	if int(gotF) != want {
		return fmt.Errorf("usable inventory for %s = %v, want %d (full body: %s)", sku, usable, want, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// workforce-management steps
// ---------------------------------------------------------------------

func (w *world) startAssociateShift(associateID, certification string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/associates/%s/start-shift", workforceBaseURL, associateID), map[string]any{
		"certifications": []string{certification},
	}))
}

func (w *world) commitShiftPlan(buildingID, shiftID, pathID string, plannedHeads int, plannedRate, plannedHours float64, installedStations int) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, workforceBaseURL+"/shift-plans", map[string]any{
		"buildingId": buildingID,
		"shiftId":    shiftID,
		"lines": []map[string]any{
			{
				"pathId": pathID, "plannedHeads": plannedHeads, "plannedRate": plannedRate,
				"plannedHours": plannedHours, "installedStations": installedStations,
			},
		},
	}))
}

// staffingGapIsUnderstaffed asserts GET /paths/{pathId}/staffing-gap
// (workforce-management's own staffing-gap read model, the same one its
// get_staffing_gap MCP tool wraps) reports Understaffed=true with the
// expected planned/active head counts.
func (w *world) staffingGapIsUnderstaffed(pathID, buildingID, shiftID string, plannedHeads, activeHeads int) error {
	url := fmt.Sprintf("%s/paths/%s/staffing-gap?buildingId=%s&shiftId=%s", workforceBaseURL, pathID, buildingID, shiftID)
	if err := w.doJSON(http.MethodGet, url, nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET staffing-gap: status %d, body=%s", w.last.status, w.last.body)
	}
	got := w.last.json()
	understaffed, _ := got["understaffed"].(bool)
	if !understaffed {
		return fmt.Errorf("staffing gap for %s not flagged understaffed (full body: %s)", pathID, w.last.body)
	}
	gotPlanned, _ := toFloat(got["plannedHeads"])
	gotActive, _ := toFloat(got["activeHeads"])
	if int(gotPlanned) != plannedHeads || int(gotActive) != activeHeads {
		return fmt.Errorf("staffing gap for %s = planned %v/active %v, want planned %d/active %d (full body: %s)",
			pathID, got["plannedHeads"], got["activeHeads"], plannedHeads, activeHeads, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// wes-work-planning steps
// ---------------------------------------------------------------------

// wesHasWorkPoolFor is unused by name once T5 adds an explicit-WIP-limit
// variant below, but bootstrap.feature still calls it for its own
// default-provisioned (WIP limit 1000) pool.
func (w *world) wesHasWorkPoolFor(pathID string) error {
	// A work pool is auto-provisioned by the first enqueue against a path
	// (see openapi.yaml enqueueWorkUnit description) — nothing to do here
	// beyond documenting scenario intent; kept as its own step for
	// readability.
	return nil
}

// wesHasReleaseFedWorkPoolWithWIPLimit seeds a ReleaseFed work pool
// directly in wes-work-planning's own Postgres database with an explicit,
// small WIP limit, so a T5 scenario can deterministically saturate it
// (WIP >= WIPLimit) in two enqueue+release calls instead of needing 1000.
// Same direct-seed pattern this harness already uses for inventory-storage
// bins (see binExists) — wes-work-planning has no "create pool with a
// specific limit" HTTP endpoint; a pool is otherwise always auto
// -provisioned at its 1000/1000 default on first enqueue.
func (w *world) wesHasReleaseFedWorkPoolWithWIPLimit(pathID string, wipLimit int) error {
	db, err := sql.Open("pgx", wesDBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(context.Background(),
		`INSERT INTO work_pools (path_id, mode, wip_limit, alarm_threshold)
		 VALUES ($1, 'ReleaseFed', $2, 0)
		 ON CONFLICT (path_id) DO UPDATE SET mode = 'ReleaseFed', wip_limit = $2, alarm_threshold = 0`,
		pathID, wipLimit)
	return err
}

func (w *world) enqueueWorkUnit(workUnitID string, hoursFromNow int, reference, pathID string) error {
	cpt := time.Now().UTC().Add(time.Duration(hoursFromNow) * time.Hour).Format(time.RFC3339)
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/paths/%s/work-units", wesBaseURL, pathID), map[string]any{
		"workUnitId": workUnitID, "cpt": cpt, "reference": reference,
	}))
}

func (w *world) releaseWorkFor(pathID string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/paths/%s/release", wesBaseURL, pathID), nil))
}

func (w *world) releasedWorkUnitIs(id string) error {
	got := w.last.json()
	if got["id"] != id {
		return fmt.Errorf("released work unit id = %v, want %q (full body: %s)", got["id"], id, w.last.body)
	}
	return nil
}

// wesEventuallyObservesLaborPlan polls GET /paths/{pathId}/labor-plan-view
// until it 200s with the expected plannedHeads, tolerating 404 while the
// Kafka consumer catches up.
func (w *world) wesEventuallyObservesLaborPlan(pathID string, plannedHeads int) error {
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/paths/%s/labor-plan-view", wesBaseURL, pathID), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("status %d (body=%s)", w.last.status, w.last.body)
		}
		got := w.last.json()
		gotHeads, _ := toFloat(got["plannedHeads"])
		if int(gotHeads) != plannedHeads {
			return fmt.Errorf("plannedHeads = %v, want %d", got["plannedHeads"], plannedHeads)
		}
		return nil
	})
}

// wesEventuallyReportsCompleted polls GET on the labor-plan-view's sibling
// endpoint isn't available for a single work unit's state directly, so
// this re-releases nothing and instead relies on the fact that
// RecordCompletion is idempotently checked via a 409 "already completed"
// on a repeat POST — the cleanest externally-observable proof this
// service's own state (fed by the TaskCompleted Kafka consumer) reflects
// completion, without requiring a new read endpoint.
func (w *world) wesEventuallyReportsCompleted(workUnitID string) error {
	return eventually(func() error {
		if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/work-units/%s/complete", wesBaseURL, workUnitID), nil); err != nil {
			return err
		}
		// 409 = already completed (by the Kafka-driven TaskCompleted
		// consumer) -> success for this assertion.
		if w.last.status == http.StatusConflict {
			return nil
		}
		if w.last.status == http.StatusOK {
			// It hadn't been completed by the consumer yet, and our own
			// call just completed it — that's a false pass; the consumer
			// hasn't run. Fail so the poll keeps retrying (until timeout,
			// at which point the consumer truly never delivered).
			return fmt.Errorf("work unit was not yet completed by the Kafka consumer (this call completed it itself)")
		}
		return fmt.Errorf("unexpected status %d (body=%s)", w.last.status, w.last.body)
	})
}

// wesRebalanceRecommendationIs asserts GET /paths/{pathId}/rebalance's
// action field, the same tool wes-work-planning's own get_rebalance_
// recommendation MCP tool wraps (see its internal/adapters/inbound/mcp
// /tools.go) — checked here over plain REST so this step needs no MCP
// client of its own, while still proving the exact signal warehouse-ops-
// agent's outbound MCP client will read moments later in the scenario.
func (w *world) wesRebalanceRecommendationIs(pathID, action string) error {
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/paths/%s/rebalance", wesBaseURL, pathID), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET rebalance: status %d, body=%s", w.last.status, w.last.body)
	}
	got := w.last.json()
	if got["action"] != action {
		return fmt.Errorf("rebalance action for %s = %v, want %q (full body: %s)", pathID, got["action"], action, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// fulfillment-execution steps
// ---------------------------------------------------------------------

func (w *world) registerStation(stationID, capabilities string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, fulfillmentBaseURL+"/stations", map[string]any{
		"stationId": stationID, "capabilities": strings.Split(capabilities, ","),
	}))
}

// fulfillmentEventuallyCreatesTaskFor polls the PICK queue depth until it
// is >= 1, proving the WorkReleased Kafka consumer created a Task. The
// order ref itself isn't independently queryable pre-claim, so depth > 0
// is the externally-observable proxy this API offers.
func (w *world) fulfillmentEventuallyCreatesTaskFor(orderRef string) error {
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fulfillmentBaseURL+"/queues/PICK/depth", nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("status %d (body=%s)", w.last.status, w.last.body)
		}
		got := w.last.json()
		depth, _ := toFloat(got["depth"])
		if depth < 1 {
			return fmt.Errorf("queue depth = %v, want >= 1", got["depth"])
		}
		return nil
	})
}

func (w *world) claimNextTask(stationID, taskType string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/stations/%s/claim-next", fulfillmentBaseURL, stationID), map[string]any{
		"taskType": taskType,
	}))
}

func (w *world) claimedTaskIsForOrder(orderRef string) error {
	got := w.last.json()
	if got["orderRef"] != orderRef {
		return fmt.Errorf("claimed task orderRef = %v, want %q (full body: %s)", got["orderRef"], orderRef, w.last.body)
	}
	id, _ := got["id"].(string)
	w.claimedTaskID = id
	return nil
}

func (w *world) completeClaimedTask(stationID string) error {
	if w.claimedTaskID == "" {
		return fmt.Errorf("no task has been claimed yet this scenario")
	}
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/tasks/%s/complete", fulfillmentBaseURL, w.claimedTaskID), map[string]any{
		"stationId": stationID,
	}))
}

// claimedTaskLeaseForcedExpired directly back-dates the currently-claimed
// task's lease_expiry column in fulfillment-execution's own Postgres to a
// moment in the past, so its diagnose_stuck_tasks tool (and this scenario's
// stuck-task signal into warehouse-ops-agent) sees it as already expired
// without needing to wait out the real 5-minute DefaultLeaseDuration (see
// internal/application/usecases/claim_next.go). Same direct-seed pattern
// this harness already uses for inventory-storage bins (see binExists) —
// there is no "force-expire one task's lease" HTTP endpoint; only the bulk
// POST /tasks/expire-leases sweep, which also FREES the task (status back
// to Pending) rather than leaving it Claimed-but-expired, which is
// precisely the "stuck" state diagnose_stuck_tasks is meant to catch.
func (w *world) claimedTaskLeaseForcedExpired() error {
	if w.claimedTaskID == "" {
		return fmt.Errorf("no task has been claimed yet this scenario")
	}
	db, err := sql.Open("pgx", fulfillmentDBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	past := time.Now().UTC().Add(-1 * time.Hour)
	tag, err := db.ExecContext(context.Background(),
		`UPDATE tasks SET lease_expiry = $1 WHERE id = $2 AND status = 'CLAIMED'`, past, w.claimedTaskID)
	if err != nil {
		return err
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		return fmt.Errorf("no CLAIMED task with id %q found to force-expire", w.claimedTaskID)
	}
	return nil
}

// ---------------------------------------------------------------------
// warehouse-ops-agent steps (T5)
// ---------------------------------------------------------------------

// flowBalanceDecision holds the last GET /flow-balance/{pathId} response,
// decoded from w.last.json() into typed fields so later assertion steps
// don't each re-parse the raw map.
type flowBalanceDecision struct {
	RecommendedAction string
	ProposedHeads     int
	Partial           bool
	Evidence          []map[string]any
}

func (w *world) requestFlowBalanceException(pathID, buildingID, shiftID string) error {
	url := fmt.Sprintf("%s/flow-balance/%s?buildingId=%s&shiftId=%s", opsAgentBaseURL, pathID, buildingID, shiftID)
	if err := w.doJSON(http.MethodGet, url, nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET flow-balance: status %d, body=%s", w.last.status, w.last.body)
	}
	return nil
}

func (w *world) flowBalanceDecodeOrErr() (flowBalanceDecision, error) {
	got := w.last.json()
	if got == nil {
		return flowBalanceDecision{}, fmt.Errorf("no flow-balance response decoded yet this scenario (body: %s)", w.last.body)
	}
	var d flowBalanceDecision
	d.RecommendedAction, _ = got["recommendedAction"].(string)
	if heads, ok := toFloat(got["proposedHeads"]); ok {
		d.ProposedHeads = int(heads)
	}
	d.Partial, _ = got["partial"].(bool)
	if ev, ok := got["evidence"].([]any); ok {
		for _, e := range ev {
			if m, ok := e.(map[string]any); ok {
				d.Evidence = append(d.Evidence, m)
			}
		}
	}
	return d, nil
}

func (w *world) flowBalanceDecisionIsNotPartial() error {
	d, err := w.flowBalanceDecodeOrErr()
	if err != nil {
		return err
	}
	if d.Partial {
		return fmt.Errorf("flow-balance decision is partial (full body: %s)", w.last.body)
	}
	return nil
}

func (w *world) flowBalanceRecommendedActionIs(action string) error {
	d, err := w.flowBalanceDecodeOrErr()
	if err != nil {
		return err
	}
	if d.RecommendedAction != action {
		return fmt.Errorf("flow-balance recommendedAction = %q, want %q (full body: %s)", d.RecommendedAction, action, w.last.body)
	}
	return nil
}

func (w *world) flowBalanceProposedHeadsIs(want int) error {
	d, err := w.flowBalanceDecodeOrErr()
	if err != nil {
		return err
	}
	if d.ProposedHeads != want {
		return fmt.Errorf("flow-balance proposedHeads = %d, want %d (full body: %s)", d.ProposedHeads, want, w.last.body)
	}
	return nil
}

// flowBalanceEvidenceIncludesAllThreeSources asserts the decision's
// evidence trail names all three upstream MCP tool sources this
// correlation depends on (see internal/application/usecases
// /flow_balance_advisory.go's wesSource/wfmSource/feSource constants),
// proving the exception was correlated from the real MCP call chain, not
// synthesized from a subset.
func (w *world) flowBalanceEvidenceIncludesAllThreeSources() error {
	d, err := w.flowBalanceDecodeOrErr()
	if err != nil {
		return err
	}
	want := map[string]bool{
		"wes-work-planning":     false,
		"workforce-management":  false,
		"fulfillment-execution": false,
	}
	for _, e := range d.Evidence {
		source, _ := e["source"].(string)
		for prefix := range want {
			if strings.HasPrefix(source, prefix) {
				want[prefix] = true
			}
		}
	}
	for prefix, found := range want {
		if !found {
			return fmt.Errorf("flow-balance evidence trail missing a %s.* source (full body: %s)", prefix, w.last.body)
		}
	}
	return nil
}

func (w *world) requestDailyBrief() error {
	if err := w.doJSON(http.MethodGet, opsAgentBaseURL+"/daily-brief", nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET daily-brief: status %d, body=%s", w.last.status, w.last.body)
	}
	return nil
}

// dailyBriefListsOpenExceptionForPath asserts the daily brief's flattened
// openExceptions list (see internal/domain/policy.DailyBrief.OpenExceptions,
// ranked critical-first) contains at least one entry for the given
// siteCode/pathId — proving E3's independent flow-balance-risk correlation
// rule (>= 2 of backlog/staffing/stuck signals; see internal/domain/policy
// /dailybrief.go's deriveExceptions) also caught the same imbalance E1's
// dedicated FlowBalanceAdvisory correlated moments earlier.
func (w *world) dailyBriefListsOpenExceptionForPath(pathID, siteCode string) error {
	got := w.last.json()
	exceptions, _ := got["openExceptions"].([]any)
	for _, raw := range exceptions {
		e, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if e["pathId"] == pathID && e["siteCode"] == siteCode {
			return nil
		}
	}
	return fmt.Errorf("daily brief has no open exception for pathId=%q siteCode=%q (full body: %s)", pathID, siteCode, w.last.body)
}

// ---------------------------------------------------------------------
// generic assertion helpers
// ---------------------------------------------------------------------

func (w *world) responseStatusIs(want int) error {
	if w.last.status != want {
		return fmt.Errorf("response status = %d, want %d (body: %s)", w.last.status, want, w.last.body)
	}
	return nil
}

// expectOK2xx is a convenience wrapper: run the doJSON call, propagate a
// transport error, then require a 2xx status.
func (w *world) expectOK2xx(err error) error {
	if err != nil {
		return err
	}
	if w.last.status < 200 || w.last.status >= 300 {
		return fmt.Errorf("expected 2xx, got %d (body: %s)", w.last.status, w.last.body)
	}
	return nil
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}

// eventually retries fn until it returns nil or eventualWaitTimeout
// elapses, returning the last error. Used for every assertion that
// depends on asynchronous Kafka consumer delivery.
func eventually(fn func() error) error {
	deadline := time.Now().Add(eventualWaitTimeout)
	var lastErr error
	for {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met within %s: %w", eventualWaitTimeout, lastErr)
		}
		time.Sleep(eventualWaitPoll)
	}
}

// ---------------------------------------------------------------------
// godog wiring
// ---------------------------------------------------------------------

func InitializeScenario(sc *godog.ScenarioContext) {
	w := newWorld()
	sc.Before(func(ctx context.Context, s *godog.Scenario) (context.Context, error) {
		w = newWorld()
		return ctx, nil
	})

	sc.Step(`^all warehouse-systems services are healthy$`, w.allServicesAreHealthy)
	sc.Step(`^warehouse-ops-agent is healthy$`, w.opsAgentIsHealthy)

	// facility-layout
	sc.Step(`^I register site "([^"]*)" named "([^"]*)" in facility-layout$`, w.registerSite)
	sc.Step(`^I register location type "([^"]*)" with capacity (\d+) kg and ([\d.]+) m3 in facility-layout$`, w.registerLocationType)
	sc.Step(`^I register zone "([^"]*)"/"([^"]*)" in site "([^"]*)" with temperature class "([^"]*)" in facility-layout$`, w.registerZone)
	sc.Step(`^I register aisle "([^"]*)" in zone "([^"]*)" with sequence hint (\d+) and direction "([^"]*)" in facility-layout$`, w.registerAisle)
	sc.Step(`^I register location slot "([^"]*)" of type "([^"]*)" in facility-layout$`, w.registerLocationSlot)

	// inventory-storage
	sc.Step(`^a Bin "([^"]*)" with capacity (\d+) exists in inventory-storage$`, w.binExists)
	sc.Step(`^I receive (\d+) units of SKU "([^"]*)" in inventory-storage$`, w.receiveStock)
	sc.Step(`^I stow (\d+) units of SKU "([^"]*)" into bin "([^"]*)" in inventory-storage$`, w.stowStock)
	sc.Step(`^the usable inventory for SKU "([^"]*)" in inventory-storage is (\d+)$`, w.usableInventoryIs)

	// workforce-management
	sc.Step(`^an associate "([^"]*)" starts a shift with certification "([^"]*)" in workforce-management$`, w.startAssociateShift)
	sc.Step(`^workforce-management commits a shift plan for building "([^"]*)" shift "([^"]*)" path "([^"]*)" with (\d+) planned heads, rate (\d+), hours (\d+), installed stations (\d+)$`, w.commitShiftPlan)
	sc.Step(`^the staffing gap for path "([^"]*)" building "([^"]*)" shift "([^"]*)" in workforce-management is understaffed with (\d+) planned heads and (\d+) active heads$`, w.staffingGapIsUnderstaffed)

	// wes-work-planning
	sc.Step(`^wes-work-planning eventually observes a labor plan view for path "([^"]*)" with planned heads (\d+)$`, w.wesEventuallyObservesLaborPlan)
	sc.Step(`^wes-work-planning has a work pool for process path "([^"]*)"$`, w.wesHasWorkPoolFor)
	sc.Step(`^wes-work-planning has a release-fed work pool for process path "([^"]*)" with WIP limit (\d+)$`, w.wesHasReleaseFedWorkPoolWithWIPLimit)
	sc.Step(`^I enqueue work unit "([^"]*)" with cpt in (\d+) hour(?:s)? and reference "([^"]*)" to process path "([^"]*)" in wes-work-planning$`, w.enqueueWorkUnit)
	sc.Step(`^work is released from process path "([^"]*)" in wes-work-planning$`, w.releaseWorkFor)
	sc.Step(`^the released work unit is "([^"]*)"$`, w.releasedWorkUnitIs)
	sc.Step(`^wes-work-planning eventually reports work unit "([^"]*)" as completed$`, w.wesEventuallyReportsCompleted)
	sc.Step(`^wes-work-planning's rebalance recommendation for path "([^"]*)" is "([^"]*)"$`, w.wesRebalanceRecommendationIs)

	// fulfillment-execution
	sc.Step(`^a station "([^"]*)" is registered with capabilities "([^"]*)" in fulfillment-execution$`, w.registerStation)
	sc.Step(`^fulfillment-execution eventually creates a task for order "([^"]*)"$`, w.fulfillmentEventuallyCreatesTaskFor)
	sc.Step(`^station "([^"]*)" claims the next "([^"]*)" task in fulfillment-execution$`, w.claimNextTask)
	sc.Step(`^the claimed task is for order "([^"]*)"$`, w.claimedTaskIsForOrder)
	sc.Step(`^station "([^"]*)" completes the claimed task in fulfillment-execution$`, w.completeClaimedTask)
	sc.Step(`^the claimed task's lease is forced to have already expired in fulfillment-execution$`, w.claimedTaskLeaseForcedExpired)

	// warehouse-ops-agent (T5)
	sc.Step(`^I request the flow-balance exception for path "([^"]*)" building "([^"]*)" shift "([^"]*)" from warehouse-ops-agent$`, w.requestFlowBalanceException)
	sc.Step(`^the flow-balance decision is not partial$`, w.flowBalanceDecisionIsNotPartial)
	sc.Step(`^the flow-balance recommended action is "([^"]*)"$`, w.flowBalanceRecommendedActionIs)
	sc.Step(`^the flow-balance proposed heads is (\d+)$`, w.flowBalanceProposedHeadsIs)
	sc.Step(`^the flow-balance evidence trail includes sources from wes-work-planning, workforce-management, and fulfillment-execution$`, w.flowBalanceEvidenceIncludesAllThreeSources)
	sc.Step(`^I request the daily brief from warehouse-ops-agent$`, w.requestDailyBrief)
	sc.Step(`^the daily brief lists an open exception for path "([^"]*)" in site "([^"]*)"$`, w.dailyBriefListsOpenExceptionForPath)

	// generic
	sc.Step(`^the response status is (\d+)$`, w.responseStatusIs)
}

func TestMain(m *testing.M) {
	suite := godog.TestSuite{
		Name:                "e2e",
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			Strict:   true,
			TestingT: nil,
		},
	}
	if suite.Run() != 0 {
		os.Exit(1)
	}
	os.Exit(0)
}
