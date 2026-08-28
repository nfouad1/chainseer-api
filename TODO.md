# TODO

## Robinhood Chain

- [x] Add source-aware early-rug entry protection without raising the non-predictive score floor.
  - Quarantine a DEX source after at least five closes when total losses reach 35% over its latest 20 closes.
  - Enforce explicit raw-safety refusal verdicts while retaining missing evidence as a visible abstention.
  - Recheck both protections at the final paper-position opening boundary.
  - Keep V4 paper admission disabled; continue collecting its flow signals in shadow mode.

- [x] Harden concurrent live scans and scheduled learning against shared RPC/API contention.
  - Add bounded retry with exponential backoff and jitter around observer RPC calls and external price-provider requests.
  - Advance discovery cursors only after the complete block window succeeds.
  - Preserve the full Python traceback in scheduler status and log files instead of only the first line.
  - Add a regression test that runs a live Robinhood token scan alongside a scheduled learning cycle and simulates throttling/timeouts.
  - Verify both lanes recover without losing candidates, duplicating promotions, or leaving a run marked `running`.

- [x] Add a V4 position-custody verifier as prospective shadow evidence.
  - Index canonical V4 PositionManager NFT transfers and map packed position ranges to full pool IDs.
  - Measure current owner, token-specific approval, owner bytecode, recognized future timelock, and active in-range liquidity.
  - Treat unmanaged or stale core liquidity as missing coverage, never as locked liquidity.
  - Keep custody, hook, and executable round-trip gates independent; keep V4 paper admission disabled.
  - Bound and fail-open the telemetry stage so RPC throttling cannot stop the learner or user scans.

- [x] Establish the first gated Memory Core producer boundary for Robinhood learning.
  - Keep a 5,000-block live custody cursor independent from the bounded historical backfill cursor.
  - Report active-window identity coverage and explicit near-threshold Flow qualification gaps.
  - Seal only new prospective analyses into the dedicated Robinhood learning Timechain and bind outcomes to the exact analysis/evidence hash.
  - Anchor prospective outcome horizons to the immutable analysis timestamp, never an older launch/discovery timestamp.
  - Keep pre-upgrade checkpoints explicitly legacy-unbound rather than retroactively promoting them to verified training evidence.
  - Reuse the initialized dashboard store so read-only refreshes cannot rerun migrations or collide with learner writes.

- [ ] Review a V4 paper-only canary after prospective custody evidence matures.
  - Require at least 30 active pools plus adequate managed-liquidity coverage and bounded non-exit/catastrophic outcomes.
  - Index `ApprovalForAll` changes or establish a conservative operator allowlist before treating contract custody as complete.
  - Add hook-specific verification; hook absence is the only passing hook state in V1.
  - Do not use the archived V4 losses as custody-validation data because they lack block-pinned position ownership evidence.

## Production scanner

- [x] Make live scan polling resilient to temporary API and Fly health interruptions.
  - Preserve the accepted job ID when an individual status poll times out or returns a transient `5xx` response.
  - Display a reconnecting state and retry with bounded exponential backoff instead of terminating a scan near completion.
  - Recover and display a report that completed while the webpage was disconnected.
  - Reserve “No result was published” for a confirmed terminal backend failure; use an accurate temporary-connection message for polling interruptions.
  - Isolate full Timechain audits and other maintenance work from request-serving CPU, memory, and event-loop capacity.
  - Add an end-to-end regression test that interrupts polling around 90%, completes the backend job, and proves the report is recovered without starting a duplicate analysis.
  - Add production telemetry for polling timeouts, Fly health-check failures, memory pressure, reconnect success, and abandoned-but-completed jobs.

<!-- robinhood-reflection:auto:start -->
## Robinhood reflection recommendations

Generated from sealed 15-candidate or 15-signal checkpoints. Completion state is preserved on refresh.

- [ ] `RH-FLOW-CONTROL-EDGE` — Require a positive paired control advantage
  - Recommendation: Keep Flow shadow-only until the lower confidence bound of paired net expectancy is above zero.
  - Evidence: Checkpoint 165: paired 15m evidence is {'qualified': {'n': 163, 'mean_net_return': -0.3555772204337114, 'catastrophic_rate': 0.3312883435582822}, 'matched_control': {'n': 100, 'mean_net_return': -0.4363601278210417, 'catastrophic_rate': 0.41}, 'incremental_expectancy': 0.0807829073873303, 'paired_n': 100, 'paired_incremental_expectancy': 0.0704570378316384, 'paired_incremental_ci_95': [-0.06116188828919956, 0.21286553178929737]}.
  - Checkpoints: first 15, latest 165
- [ ] `RH-FLOW-EXIT-TAIL-RISK` — Reduce non-exitable and catastrophic Flow outcomes
  - Recommendation: Investigate identity concentration, adverse direction, liquidity and quote failures before promotion.
  - Evidence: Checkpoint 165: non-exit=0.17791411042944785, catastrophic=0.3312883435582822.
  - Checkpoints: first 15, latest 165
- [ ] `RH-FLOW-PROSPECTIVE-SAMPLE` — Continue the frozen prospective Flow cohort
  - Recommendation: Do not tune Flow thresholds before the pre-registered evidence window is complete.
  - Evidence: Checkpoint 90: 89 completed 15m qualified outcomes.
  - Checkpoints: first 15, latest 90
- [x] `RH-REFLECT-MARKET-COVERAGE` — Improve Robinhood outcome-market coverage
  - Recommendation: Add a fallback price/market-cap source and retain source-specific failure telemetry before using checkpoint returns for calibration.
  - Evidence: Checkpoint 2415: market observation failure ratio was 33.3%.
  - Checkpoints: first 45, latest 2415
- [ ] `RH-REFLECT-METADATA` — Backfill missing Robinhood token identity metadata
  - Recommendation: Retry name/symbol reads from a current block and add an explorer fallback so analyzed-token evidence remains attributable.
  - Evidence: Checkpoint 3495: 15 of 15 analyzed candidates lacked a name or symbol.
  - Checkpoints: first 120, latest 3495
- [ ] `RH-REFLECT-SCORE-NON-PREDICTIVE` — A higher legitimacy score is not buying a better outcome
  - Recommendation: Do not raise the entry floor on this evidence. The floor is a safety control only if score predicts outcome; measure the safety sub-signals (holder concentration, liquidity custody) against outcomes before changing any threshold.
  - Evidence: Checkpoint 3495: score-outcome correlation -0.1752 over 81 observed closes; worst bucket 75-80 averages 0.7881x over 32 closes
  - Checkpoints: first 420, latest 3495
- [ ] `RH-REFLECT-V4-CATCHUP` — Keep Robinhood V4 discovery continuously caught up
  - Recommendation: Use adaptive bounded block windows and explicit backlog telemetry; never advance the cursor across a failed window.
  - Evidence: Checkpoint 3495: V4 discovery reported 2142410 blocks behind.
  - Checkpoints: first 120, latest 3495
<!-- robinhood-reflection:auto:end -->
