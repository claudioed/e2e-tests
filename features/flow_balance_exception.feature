Feature: warehouse-ops-agent correlates a flow imbalance into a FlowBalanceException
  T5 — proves the agentic decision-support layer end-to-end against the real
  MCP servers of all 5 bounded contexts. Three INDEPENDENT signals are seeded
  directly against three different bounded contexts (wes-work-planning,
  workforce-management, fulfillment-execution) for the SAME process path;
  warehouse-ops-agent then correlates them over its own outbound MCP clients
  and must emit the expected FlowBalanceException (E1) with a ranked
  recommendation and a full evidence trail naming all three sources. The
  daily brief (E3) must also list the resulting open exception.

  Every signal is seeded on a process path ("pick-t5-imbalance") dedicated to
  this scenario, disjoint from bootstrap.feature's "pick-zone-a", so the two
  scenarios never interfere with each other regardless of execution order —
  deterministic seeding per the T5 card's guardrail.

  Background:
    Given all warehouse-systems services are healthy
    And warehouse-ops-agent is healthy

  @e2e @t5 @ops-agent
  Scenario: A saturated release-fed pool plus a staffing gap plus a stuck task correlate into an assign_labor recommendation
    # --- wes-work-planning: saturate the release-fed pool so its own
    # rebalance recommendation is ReassignLabor (WIP >= WIPLimit with
    # backlog still pending) --------------------------------------------
    Given wes-work-planning has a release-fed work pool for process path "pick-t5-imbalance" with WIP limit 1
    And I enqueue work unit "wu-t5-1" with cpt in 1 hour and reference "order-t5-1" to process path "pick-t5-imbalance" in wes-work-planning
    And I enqueue work unit "wu-t5-2" with cpt in 2 hours and reference "order-t5-2" to process path "pick-t5-imbalance" in wes-work-planning
    When work is released from process path "pick-t5-imbalance" in wes-work-planning
    Then the released work unit is "wu-t5-1"
    And wes-work-planning's rebalance recommendation for path "pick-t5-imbalance" is "ReassignLabor"

    # --- workforce-management: commit a shift plan with no assignments,
    # so the path is confirmed understaffed (0 active < planned) ---------
    Given workforce-management commits a shift plan for building "wh1" shift "shift-t5" path "pick-t5-imbalance" with 2 planned heads, rate 30, hours 8, installed stations 5
    Then the staffing gap for path "pick-t5-imbalance" building "wh1" shift "shift-t5" in workforce-management is understaffed with 2 planned heads and 0 active heads

    # --- fulfillment-execution: the WorkReleased event from wes's release
    # above (published because wes runs with EVENT_PUBLISHER=kafka) is
    # consumed here and creates a PICK task for order "wu-t5-1" (the
    # released work unit's id — see internal/adapters/inbound/kafka
    # /consumer.go's deriveTaskType/orderRef mapping). Claim it, then force
    # its lease already expired (bypassing the 5-minute default so the
    # scenario runs in seconds, not minutes) -----------------------------
    Given a station "station-t5-1" is registered with capabilities "pick" in fulfillment-execution
    When fulfillment-execution eventually creates a task for order "wu-t5-1"
    And station "station-t5-1" claims the next "PICK" task in fulfillment-execution
    Then the claimed task is for order "wu-t5-1"
    When the claimed task's lease is forced to have already expired in fulfillment-execution

    # --- warehouse-ops-agent: correlate all three signals -----------------
    When I request the flow-balance exception for path "pick-t5-imbalance" building "wh1" shift "shift-t5" from warehouse-ops-agent
    Then the flow-balance decision is not partial
    And the flow-balance recommended action is "assign_labor"
    And the flow-balance proposed heads is 2
    And the flow-balance evidence trail includes sources from wes-work-planning, workforce-management, and fulfillment-execution

    # --- warehouse-ops-agent daily brief (E3): the same correlation must
    # also surface as an open exception in the synthesized brief ----------
    When I request the daily brief from warehouse-ops-agent
    Then the daily brief lists an open exception for path "pick-t5-imbalance" in site "WH1"
