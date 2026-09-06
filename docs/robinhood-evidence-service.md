# Bounded evidence service during recovery

The recovery selector previously refused evidence until backfill's recovery ratio
met its target. A growing backlog could therefore starve economic measurements
indefinitely, despite a responsive live lane and a passed operational cohort.

After a frozen operational cohort finishes, overdue evidence now receives one
existing 90-second worker slot per 600 seconds during recovery. This is a minimum
service preference, not an exact launch-time guarantee: urgent certificates,
position-mark pacing, active workers, live-start guards and supervisor-window fit
still take precedence. Ordinary non-recovery scheduling keeps its normal cadence.

The attempt timestamp is persisted in `evidence_service_state.json` after launch;
startup also reads durable evidence lane start time. Failed attempts consume the
interval too, preventing restart-driven retry storms. No policy thresholds,
economic checkpoints, cohort samples or stored outcomes are rewritten.

Evidence cannot overlap backfill, analysis, certificate or memory workers, in either
launch direction. It remains compatible with live scans and separate position
marks. Existing RPC limits, stage budgets, per-item reserves and process deadlines
remain unchanged. Backfill receives the remaining recovery slots; it is no longer
allowed to exclude evidence forever. Monitor both actual evidence throughput and
backfill convergence before increasing this reserved share.
