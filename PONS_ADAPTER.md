# Chainseer Pons paper/shadow adapter

`chainseer_pons.py` is a Pons-specific, paper-only adapter for Robinhood Chain.
It does not contain wallet, approval, signing, router-execution, or transaction
broadcast code.

## What it verifies

Each candidate must pass an evidence-backed binding chain:

1. `TokenLaunched` came from a pinned active or legacy Pons factory.
2. `getLaunchedToken(token)` matches the event, WETH, position manager,
   position NFT, fixed supply, fee tier, and restriction end block.
3. The token's `liquidityPool()`, the event pool, and V3 factory `getPool()`
   resolve to the same pool.
4. The pool contains only the launch token and Pons WETH, uses the 1% tier,
   has active liquidity, and is unlocked.
5. The factory-resolved locker owns the exact V3 position NFT.
6. The deployed Quoter V2 can model both the configured entry and an immediate
   exit at the same pinned block.
7. The executable round-trip loss and deviation from `slot0` stay under the
   configured limits.
8. While launch protection is active, the exact quote must fit the token's
   own `maxTxAmount()` and `maxWalletAmount()` values. The model assumes a
   fresh paper wallet with zero existing balance.
9. Holder concentration excludes only the canonical pool, resolved locker,
   zero address, and burn address.

DexScreener is optional enrichment and is filtered to the canonical pool.
Other pairs are never summed into Pons liquidity.

## Analysis and admission flow

Every token is analyzed before either a paper or shadow position can be
opened. The Pons adapter deliberately uses `PonsRiskAnalyzer`, not the
monolithic `Chainseer.analyze_token()` path in `chainseer.py`, as its entry
authority. The Pons analyzer applies the same evidence-first Chainseer
discipline but is bound to the actual Pons contracts, canonical Uniswap V3
pool, locked position NFT, restrictions, holders, verified source, and
executable Quoter V2 routes. The general analyzer can be added later as
non-authoritative enrichment; it is not allowed to override Pons evidence.

One clean analysis is no longer enough to open a position. New candidates
enter a persistent admission quarantine and must satisfy all of these gates:

- two clean analyses at different block pins, spaced by at least 60 seconds;
- at least five minutes of observed survival;
- no Pons risk hard stop in the recent observation window;
- executable round-trip loss no higher than 8%;
- round-trip deterioration no greater than 2.5 percentage points;
- canonical-liquidity drawdown no greater than 20%.

There is intentionally no hard-coded USD liquidity floor in the active
admission policy yet. Liquidity floors of $3,000 and $5,000 are evaluated as
predeclared counterfactual cohorts so the historical result is learned before
a human explicitly approves any policy change.

The scheduled learning loop gives pending candidates a bounded refresh lane.
This makes the second observation and survival gate happen automatically;
there is no need to rerun candidates manually.

### Evidence-quality and scheduler v2

Required evidence has three explicit outcomes:

- `complete_safe`: all required onchain evidence was observed and the token
  passed the Pons risk gate;
- `complete_unsafe`: required evidence was observed and a token-specific
  canonicality, restriction, concentration, or executable-quote rule failed;
- `infrastructure_indeterminate`: required evidence could not be observed
  because the RPC transport, HTTP service, timeout, or rate limit failed.

Infrastructure-indeterminate analyses cannot open a position, cannot close an
existing position as a risk signal, and are excluded from admission counts and
counterfactual policy outcomes. The RPC client retries transient 429 and 5xx
responses with bounded exponential backoff, respects `Retry-After`, and records
health telemetry under `rpc_health.json`.

Admission refreshes are opportunity-ranked. A due candidate with one clean
observation is refreshed before a token already failing the quote or risk gate.
Mutable failures enter a 15-minute cooldown, infrastructure failures use
exponential retry backoff, immutable canonicality failures and expired
candidates become terminal, and repeated appearances in discovery cannot
bypass their cooldown. The scheduler policy is separately signed so operational
ranking changes cannot silently alter the admission risk policy.

### Freshness-aware caching and bounded concurrency

Pons reuses Chainseer's thread-safe, process-local HTTP cache for all upstream
JSON calls. Mutable holder and DexScreener responses retain the short
`CHAINSEER_API_CACHE_TTL` window (30 seconds by default), so admission and exit
decisions do not inherit stale cross-cycle market data.

Positively verified Blockscout source payloads also use a bounded, hash-checked
disk cache under `pons_learning/http_cache/verified_source.json`. Its default
TTL is six hours. The compact index points to Chainseer's existing
content-addressed evidence files instead of duplicating large ABI/source
payloads. Unverified responses are never persisted. The cache remains part of
provenance: every hit records the original payload hash, fetch time, and cache
layer. Controls:

- `PONS_SOURCE_CACHE_TTL_SECONDS` (default `21600`);
- `PONS_SOURCE_CACHE_SIZE` (default `256`);
- `PONS_SOURCE_CACHE_MAX_PAYLOAD_BYTES` (default `1000000`).

Within one candidate analysis, the three source checks, holder lookup, and
canonical-pool DexScreener lookup run as independent bounded worker reads
(`PONS_HTTP_EVIDENCE_WORKERS`, default `5`). Each worker owns an isolated
provenance ledger; results are merged in declared order after completion.
Candidate analysis, RPC context, admission mutations, trade ledgers, and
Timechain sealing remain strictly sequential. Setting the worker count to `1`
restores the fully serial HTTP path.

### Concentration-resistant performance evidence

Aggregate return is not treated as sufficient evidence of a durable policy.
Chainseer reports the raw modeled return alongside:

- return after removing the single best position;
- a one-position-per-tail trimmed return;
- median and interquartile position multiples;
- profitable-position count and winner rate;
- the best winner's share of all positive modeled profit.

An explicit concentration warning is raised when aggregate return is positive
but fewer than three positions are profitable, return excluding the best
position is non-positive, or one winner contributes more than 60% of positive
modeled profit.

Counterfactual policy promotion is still human-gated and now also requires at
least three profitable validation positions, positive validation return after
removing the best position, and no more than 60% of positive validation profit
from one winner. These gates affect recommendations only; they do not alter the
paper-entry policy or enable live execution.

## Run it

All commands below are Bash-compatible from the project directory.

Observe recent launches:

```bash
python -X utf8 chainseer_pons.py observe --limit 5
```

Open eligible manual paper positions:

```bash
python -X utf8 chainseer_pons.py paper-run --limit 5 --amount-eth 0.01
```

Open the uncapped counterfactual shadow cohort:

```bash
python -X utf8 chainseer_pons.py shadow-run --limit 10 --amount-eth 0.01
```

Run one restart-safe discovery, managed-paper/shadow-entry, and mark cycle:

```bash
python -X utf8 chainseer_pons.py learn-once \
  --limit 10 \
  --mark-limit 5 \
  --admission-refresh-limit 3 \
  --amount-eth 0.01
```

The managed paper lane is fail-closed. Even when a token passes canonical
analysis and admission quarantine, it opens no managed paper position unless
the active `stability_v1` policy has zero walk-forward promotion blockers.
Once armed, the portfolio controller also enforces:

- 0.10 ETH modeled starting capital;
- at most three simultaneous positions;
- at most 0.03 ETH gross exposure;
- at most three new entries per UTC day;
- a 0.01 ETH daily realized-loss breaker;
- a 10% peak-to-trough modeled drawdown breaker;
- a three-loss streak breaker; and
- a 24-hour cooldown after a loss breaker fires.

These are signed cohort constants. Changing them requires a new state root
rather than silently rewriting an existing paper experiment. Inspect the
current gate and metrics with:

```bash
python -X utf8 chainseer_pons.py managed-portfolio \
  --root pons_learning \
  --chain-root pons_chain
```

Run the decoupled fast risk guard without discovery or admission work:

```bash
python -X utf8 chainseer_pons.py guard-once \
  --guard-limit 25 \
  --root pons_learning \
  --chain-root pons_chain
```

The guard obtains one pinned block and requests executable Quoter V2 exits for
the oldest open paper positions first, then shadow positions. It enforces the
existing stop-loss, trailing-exit, take-profit, and maximum-hold rules. It does
not infer contract risk: suspicious-token exits remain authorized only by the
periodic full canonical analysis. A failed quote is recorded as an attempt and
does not make the position's last successful mark appear fresh.

Install the paper-only Windows learning task (every 10 minutes by default):

```powershell
.\manage_chainseer_pons_learning_task.ps1 install
.\manage_chainseer_pons_learning_task.ps1 run-now
.\manage_chainseer_pons_learning_task.ps1 status
```

The task uses `IgnoreNew`, so a slow cycle never overlaps the next one. It
writes daily logs under `pons_learning/logs/`, a machine-readable
`scheduler_status.json`, and preserves all runtime data if it is later removed:

The scheduled profile is deliberately risk-first: two discovery candidates,
one admission refresh, two bounded factory chunks, and one full canonical refresh of the oldest open
position. Frequent lifecycle quotes are delegated to the separate fast guard,
so factory backfill cannot monopolize stop-loss monitoring.

```powershell
.\manage_chainseer_pons_learning_task.ps1 uninstall
```

Install the independent paper-only fast guard (every two minutes by default):

```powershell
.\manage_chainseer_pons_guard_task.ps1 install
.\manage_chainseer_pons_guard_task.ps1 run-now
.\manage_chainseer_pons_guard_task.ps1 status
```

The guard shares the mutation lock with the full learner. If a discovery cycle
owns the lock, the guard exits successfully as `skipped_busy`; it never races
the paper ledgers, cohort state, or Timechain. Use `disable` to pause it while
preserving all state, and `enable` to resume it.

Open the local, read-only state and integrity dashboard:

```bash
python -X utf8 chainseer_pons.py dashboard \
  --root pons_learning \
  --chain-root pons_chain
```

Then visit `http://127.0.0.1:8766`. The dashboard reads the actual operational
ledgers, learning summary, scheduler state, Timechain, and faculty registry. It
also shows the admission queue, its active gates, the fixed counterfactual
policy grid, RPC retry health, tri-state evidence counts, scheduler lanes, and
raw versus concentration-resistant cohort returns, plus any human-review
recommendation. It contains no example positions and exposes no mutation or
live-trading endpoint.

Inspect state:

```bash
python -X utf8 chainseer_pons.py positions
python -X utf8 chainseer_pons.py shadow-positions
python -X utf8 chainseer_pons.py admission
python -X utf8 chainseer_pons.py policy-learning
python -X utf8 chainseer_pons.py managed-portfolio
python -X utf8 chainseer_pons.py pipeline
```

Verify the operational ledgers and Timechain:

```bash
python -X utf8 chainseer_pons.py verify
```

Use `--json` for machine-readable output. Use `--no-timechain` only for
disposable tests; normal analysis uses the Timechain cognitive loop.

## Discovery behavior

The first run indexes the latest 10,000 blocks from the active factory in
1,000-block chunks. It atomically checkpoints the cursor and catalog after
every successful chunk, so an RPC timeout loses at most the in-flight chunk.
Later runs continue from the saved cursor.

The retired legacy factory is supported but not backfilled by default. Add
`--include-legacy` when its final historical window is needed:

```bash
python -X utf8 chainseer_pons.py observe --include-legacy --limit 5
```

## Timechain behavior

Normal runs use a private persistent registry under `pons_chain/registry`.
For every analysis the adapter:

- screens a bounded, trusted structured input through the covenant membrane;
- labels active senses and modalities;
- retrieves relevant rings;
- PoQ-gates and seals the evidence-backed analysis;
- runs Cambium gap detection and faculty growth;
- seals a registry epoch if faculties change;
- seals a cognitive-completion ring;
- verifies the chain and registry integrity.

Paper and shadow buy/sell events are also sealed from their hash-linked
operational event hashes. Fast guard-cycle summaries are sealed as
`pons_quote_guard` rings, while their pinned quote provenance remains in the
local evidence store. Token names, descriptions, socials, raw API bodies, and
private data are excluded from the cognitive learning input.

Admission observations, managed portfolio gate evaluations, and
counterfactual policy evaluations are sealed too.
The policy grid uses a fixed chronological 70/30 evaluation split, labels its
quote-ratio valuation as a counterfactual rather than an executable fill, and
cannot silently mutate the active admission policy. No variant can even become
a recommendation until it has at least 30 closed observations overall and 10
closed observations in the validation segment, produces a positive validation
return, and beats the control by at least 10 percentage points. A passing
variant still requires explicit human approval and is never auto-adopted.

## Safety boundary

`broadcast_live_trade()` always raises `LiveExecutionDisabledError`.

The module has no code for:

- private keys or seed phrases;
- allowances or approvals;
- transaction calldata for the swap router;
- signing;
- mempool submission;
- transaction broadcasting.

This adapter is a data-collection and strategy-validation stage. Live
execution must remain a separate future component with independent review,
capital limits, wallet isolation, transaction simulation, and an emergency
kill switch.
