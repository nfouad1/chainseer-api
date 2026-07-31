# Chainseer Base prototype

This is an observation and paper-trading prototype for new Virtuals launches on
Base. It contains no private-key loading, token approvals, transaction signing,
or transaction broadcasting.

## Run it

Use Python in UTF-8 mode so Timechain rings remain portable on Windows:

```powershell
python -X utf8 chainseer_base.py observe --limit 5
python -X utf8 chainseer_base.py paper-run --limit 5 --amount-virtual 10
python -X utf8 chainseer_base.py learn-once --limit 5 --amount-virtual 1
python -X utf8 chainseer_base.py positions
python -X utf8 chainseer_base.py shadow-positions
python -X utf8 chainseer_base.py shadow-summary --root base_learning --chain-root chainseer_chain
python -X utf8 chainseer_base.py verify
```

`observe` analyzes launches without opening positions. `paper-run` may create a
simulated position only when all hard stops pass, the score meets the configured
threshold, a paper-eligible price is available, and the observation window has
elapsed. Prototype pricing prefers a block-pinned BONDING_V5 reserve spot after
verifying that the official pair binds the candidate token to VIRTUAL. The paper
amount must be no more than 0.25% of the observed VIRTUAL reserve. A
market-cap-derived price may still be shown for analysis, but it is never used
for a paper entry.

Set `CHAINSEER_BASE_RPC_URL` to a production Base RPC endpoint when sustained
polling is needed. The public Base endpoint is the conservative default.

## Automatic learning cycle

`learn-once` is a restart-safe, paper-only cycle intended for Windows Task
Scheduler. Each invocation:

1. discovers the newest Base prototypes;
2. seals at most one initial prediction per Virtuals project ID;
3. attempts policy-gated paper entry without any live signing or broadcast;
4. refreshes tracked projects through the official project-by-ID endpoint;
5. detects lifecycle or token-address migration and records it separately;
6. records due outcome checkpoints at 5m, 1h, 6h, 24h, 7d and 30d; and
7. writes `learning_summary.json` for human review.

The learning cycle also maintains a separate counterfactual shadow cohort.
Shadow positions use the same risk gate, executable-price requirement,
observation and anti-sniper waits, modeled tax/slippage, stop loss, take-profit
tiers, trailing exit, and maximum hold as the five-position paper portfolio.
Only the portfolio-capacity gate is removed. Shadow state and events are stored
separately, cannot trigger live execution, and never change portfolio balances.
This lets Chainseer learn from every otherwise eligible launch even when all
five realistic paper slots are occupied.

Each learning cycle also writes `shadow_performance.json`. It values every open
shadow position from the newest comparable checkpoint or trader mark, applies
the configured exit tax and slippage to avoid overstating liquidation value,
and separates modeled open performance from realized closed outcomes. The
report includes cohort coverage, mark freshness, maturity, exit reasons,
win/loss counts, and performance grouped by entry score, risk level, and
Blockscout source-verification evidence. It shows gross price movement beside
the modeled flat-price round-trip friction baseline, so tax/slippage drag is
not mistaken for adverse market movement. Run `shadow-summary` for a concise
human view or add `--json` for the complete auditable schema:

```powershell
python -X utf8 chainseer_base.py shadow-summary --root base_learning --chain-root chainseer_chain
python -X utf8 chainseer_base.py shadow-summary --root base_learning --chain-root chainseer_chain --json
```

`review_status=collecting` remains in force until at least 30 shadow positions
have closed. That threshold only makes the sample eligible for strategy review;
it never enables live execution or represents statistical proof of profitability.

Fast discovery and slow tracked-project analysis use separate cadences. Seeing
an already tracked project in the newest-launch feed does not force another
security analysis; its persisted `next_refresh_at` remains authoritative. Each
scheduled cycle preserves a three-slot checkpoint-first/general lane. It adds
an oldest-mark-first shadow lane for due open positions, deduplicating projects
already selected by the regular lane. The automatic shadow target is
`ceil(open shadow positions * 2 minutes / 30 minutes)`. Chainseer plans the
three checkpoint/general slots first, counts their open-shadow overlap, and
then adds oldest-due shadow projects until that requirement is met. The normal
addition cap remains four. In automatic mode, one adaptive headroom slot is
allowed only when the four-slot plan projects a deficit and the previous cycle
finished without errors in at most 75 seconds. The hard maximum is therefore
eight tracked analyses; explicit `--shadow-refresh-limit` values disable the
adaptive slot. Regular-lane projects that also have open shadow positions count
toward the freshness requirement. The summary separately
exposes the required, selected and completed shadow marks, the dedicated
priority target, baseline deficit, adaptive eligibility/use/refusal reason,
effective selection/completion shortfalls, stale marks,
median/p95/oldest open-mark ages and cycle duration, so freshness pressure
cannot silently create scheduler overlap.

To override the automatic target for a supervised experiment, pass
`--shadow-refresh-limit N`; `0` disables the additive lane for that invocation.
Due outcome checkpoints retain their original priority and general tracked
refresh capacity is not consumed by the added shadow lane.

Run it manually once to verify the environment:

```powershell
python -X utf8 chainseer_base.py learn-once --limit 5 --amount-virtual 1
```

Install the included Windows Task Scheduler definition after the manual run
succeeds:

```powershell
powershell -ExecutionPolicy Bypass -File .\manage_chainseer_learning_task.ps1 install
powershell -ExecutionPolicy Bypass -File .\manage_chainseer_learning_task.ps1 status
```

The task runs every two minutes with the current user's limited privileges and
only while that user is logged on. It ignores a new trigger when the previous
cycle is still active, enforces a 20-minute execution limit, and writes daily
logs below `base_learning/logs/`. Chainseer also uses `.learn_once.lock` as a
second overlap guard and removes the lock when a cycle completes. A lock older
than 30 minutes is treated as stale.

To trigger a supervised cycle immediately or remove the scheduler definition:

```powershell
powershell -ExecutionPolicy Bypass -File .\manage_chainseer_learning_task.ps1 run-now
powershell -ExecutionPolicy Bypass -File .\manage_chainseer_learning_task.ps1 uninstall
```

Uninstalling the task preserves the learning database, paper ledger, evidence,
logs, and Timechain. The installed trigger is valid for ten years; reinstall it
before that period ends to renew the trigger.

Predictions and outcomes are intentionally separate. A checkpoint links back to
the immutable initial analysis ring rather than modifying it. When Base
graduation changes the token address, raw price return is marked non-comparable
until a conversion ratio has been independently verified.

## Safety controls

- Discovery is filtered to `BASE` through the official Virtuals API.
- Every analysis is pinned to a Base block and checks deployed bytecode.
- Prototype reserve prices validate the pair's token ordering on-chain before
  calculating `VIRTUAL reserve / token reserve`.
- GoPlus, Base Blockscout, and DexScreener enrich the risk decision.
- Honeypot, sell-blocking, scam, missing-code, and unexpected-factory signals
  are hard stops.
- Active Virtuals anti-sniper metadata imposes a 98-minute paper-entry delay.
- Upstream creator records are removed before local evidence or Timechain data
  is persisted.
- Paper events form an append-only hash-linked ledger verified by `verify`.
- Live execution is disabled in both policy and code.

## Evidence and state

Runtime files are written below `base_prototype/` by default:

- `last_run.json`: latest redacted analysis results
- `analysis_evidence/`: content-addressed security responses
- `observer_evidence/`: redacted launch and price observations
- `paper_events.jsonl`: hash-linked simulated trade events
- `paper_state.json`: current simulated positions
- `shadow_events.jsonl`: hash-linked counterfactual buy/sell events
- `shadow_state.json`: uncapped counterfactual positions, isolated from the portfolio
- `shadow_performance.json`: friction-aware performance, evidence coverage, maturity, and review readiness
- `freshness_chronosynaptic.json`: model-scored scheduling perspectives and the selected two-lane policy
- `learning.sqlite3`: restart-safe projects, checkpoints, migrations and runs
- `learning_summary.json`: latest learning-cycle status for daily review

The Base contract constants are pinned to the official Virtuals SDK commit
recorded at the top of `chainseer_base.py`. The SDK router quote currently
reverts for observed BONDING_V5 launches, so Chainseer uses the pair's verified
reserve ratio and labels it as a spot observation—not a guaranteed fill.
Factory-event decoding and a canonical BONDING_V5 amount-out interface remain
prerequisites for any later live-execution phase.

## Official references

- Virtuals trade SDK: https://github.com/Virtual-Protocol/vp-trade-sdk
- Virtuals launch documentation: https://whitepaper.virtuals.io/builders-hub/agent-launch-mechanisms/more-on-standard-launch
- Base network information: https://docs.base.org/base-chain/network-information
