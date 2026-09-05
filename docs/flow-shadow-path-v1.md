# Flow exit strategy shadow experiment

This experiment evaluates prospective Robinhood Uniswap V4 signals using archive
quotes at block checkpoints frozen when each event is captured. It has no order,
signing, or policy-promotion capability. Existing paper and source-admission gates
remain authoritative.

## Frozen experiment

- Select 20% of qualified event IDs deterministically; matched controls inherit
  the signal's selection. Previously recorded events are never enrolled later.
- Freeze a probe of 0.03 WETH (18 decimals) or 100 USDG (6 decimals), independent
  of the ETH/USD price when a delayed entry quote is resolved.
- Anchor entry at the event's decision head block. Schedule 18 checkpoints through
  seven nominal days, using ten blocks per second and a 20-block finality delay.
- Resolve at most four quotes per evidence cycle, within the existing deadline.
  Provider failures retry; missing evidence is never booked as a trading loss.
- Preserve unsellable entries as explicit attrition. Never treat their terminal
  accounting rows as successful market quotes.

## Strategy comparison

The preregistered family contains a 15-minute hold, staged 2x/3x sales with a
runner, a fixed 3x exit, and a trailing exit. Triggers use executable liquidation
multiples. Quotes retain the exact entry token quantity in integer base units.

Select once after 100 complete, entry-marketable training paths. Seal the selected
policy, training membership and timestamp. Holdout signals must be born after
that timestamp and belong to pools absent from the training set. Minimum review
evidence is 50 holdout paths and 30 matched pairs. A positive result also requires
positive lower confidence bounds, bounded nonexit/catastrophic rates, operational
proof and source admission. Promotion is always disabled in this version.

Partial proceeds are modeled pro rata from the full-position quote. The 100 bps
friction allowance is an assumption, not measured gas, MEV or fill latency.
Checkpoint labels are nominal times, not guarantees about wall-clock block rate.
Confidence intervals are exploratory; repeated-pool dependence and missing-path
attrition require review before any execution experiment. No profit claim follows
from an operational pass or from selecting a training winner.

## Operation and verification

`flow_shadow_paths` and its schedules are immutable. Terminal measurements form
an append-only hash chain; retries are separate mutable state. Evaluation reads
one SQLite snapshot and verifies event, schedule, block and measurement bindings.
The policy definition includes all thresholds and is hashed with each schedule.

Run an offline report with:

```powershell
.\.venv\Scripts\python.exe -X utf8 chainseer_robinhood.py shadow-exit-once --root robinhood_learning
```

The scheduled learner refreshes the report hourly after its lane supervisor
finishes, using a child process capped at 30 seconds. Reports and the one-time
training selection are recorded in `flow_shadow_exit_v1.sqlite3`; the dashboard
reads `flow_shadow_exit_v1.json` and current collection counters.

The dashboard separates current collection from completed strategy evidence.
An empty experiment correctly reports `collecting_training_paths`.

### Native-currency and process-exit repair

V4 represents native currency with address zero ([CurrencyLibrary](https://github.com/Uniswap/v4-core/blob/main/src/types/Currency.sol)).
The shadow quote adapter supports that currency only after checking both pool
currencies and the frozen anchor. It uses 18 native decimals, makes no native
ERC20 metadata calls, and leaves native market cap unknown. Ordinary candidate
admission and V4 paper/live restrictions are unchanged. Existing sampled paths
retain their original entry blocks, schedules, and matched controls.

Collector integrity errors remain recorded and cannot be overwritten by a
subsequent transport error. A later verified, hash-chained quote at the same
checkpoint resolves the outstanding error; an unverified quote cannot do so.
The counters distinguish historical, resolved, and outstanding errors. No old
measurement or failed evaluation report is deleted or rewritten.

The supervisor reconciles exited children before removing them, including at
shutdown. An unfinished database row becomes a failure even if the process
returns zero, retaining its exit code and last stage. Completed rows and newer
owners are untouched. This closes an observability/ownership gap; it does not
establish why the original worker died or guarantee no future process failures.
The old 99/100 cohort remains an unsuccessful cohort.
