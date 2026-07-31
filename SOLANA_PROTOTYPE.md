# Chainseer Solana paper/shadow prototype

`chainseer_solana.py` is an isolated, paper-only Solana adapter. Its first
launch ecosystem is Pump.fun. It uses Solana RPC for primary protocol state,
DexScreener for secondary-market observability, and Jupiter Swap v2 for
two-way execution evidence.

It contains no private-key reader, signer, transaction submission, or
broadcast implementation. A public wallet address may be supplied only so
Jupiter can attempt to assemble an unsigned buy transaction. The program never
asks for or accepts that wallet's private key.

## Evidence pipeline

1. Discover confirmed Pump program transactions with Solana JSON-RPC.
2. Decode the official Pump `CreateEvent` discriminator and Borsh fields.
3. Bind the mint and bonding curve to the Pump program.
4. Read mint/freeze authorities and Token-2022 extensions.
5. Calculate holder concentration after excluding Pump bonding-curve inventory.
6. Keep incomplete bonding curves in the `launch_observation` cohort.
7. For a completed curve, verify zero real token reserves and an index-0
   canonical PumpSwap pool on-chain.
8. Require DexScreener to expose that exact canonical pool before treating the
   secondary market as observable.
9. Only then request Jupiter metadata and a SOL→token→SOL round-trip quote.
10. Admit only `graduated_market_ready` decisions to paper/shadow trading.
11. Record the decision, evidence state, admission state, and paper/shadow event in hash-linked
   ledgers and the dedicated Solana Timechain.

The Pump event and account layouts are pinned to official
`pump-fun/pump-public-docs` commit
`9c82f61cb711b044a17f770ab8ce9f9bdf78f333`.

## Configuration

Optional but recommended Jupiter credential:

```bash
export JUPITER_API_KEY="your-jupiter-api-key"
```

Without a key, Chainseer uses Jupiter's documented keyless access and paces
requests to the lower keyless limit. With a key, it sends `x-api-key` and uses
the conservative free-tier cadence. Both modes retry bounded transient
failures; neither stores or logs the credential value.

Recommended RPC endpoint:

```bash
export CHAINSEER_SOLANA_RPC_URL="https://your-solana-rpc"
```

Optional explicitly configured failover endpoints are separated by semicolons:

```bash
export CHAINSEER_SOLANA_RPC_FALLBACK_URLS="https://rpc-two;https://rpc-three"
```

The transport rotates to another endpoint on a failed attempt, applies
`Retry-After`, exponential backoff with jitter, and a bounded circuit breaker.
Persisted health contains only credential-safe endpoint origins plus cumulative
per-method attempts, failures, retries, and status codes. RPC paths, query
strings, and credential-bearing exception URLs are never written to state,
logs, dashboard JSON, or Timechain context.

Optional watch-only public address for unsigned transaction assembly:

```bash
export SOLANA_PAPER_TAKER="your-public-solana-address"
```

`SOLANA_PAPER_TAKER` is not a secret. No private key should be configured.

## Commands

Observe recent Pump.fun launches without opening positions:

```bash
python -X utf8 chainseer_solana.py observe --limit 5 --signature-limit 100
```

Run one discovery, analysis, shadow-admission, mark, and promotion cycle:

```bash
python -X utf8 chainseer_solana.py learn-once --limit 10 --signature-limit 100 --recovery-limit 3
```

Every infrastructure-indeterminate token is placed in
`solana_learning/recovery_queue.json`. A later cycle re-analyzes due entries
after a restart, using bounded exponential scheduling. Reanalysis appends a
`solana_risk_reanalysis` ledger event and a new Timechain ring; it never
rewrites the original observation. The token's current materialized state moves
forward only after the new evidence is recorded.

Inspect status and promotion evidence:

```bash
python -X utf8 chainseer_solana.py status
python -X utf8 chainseer_solana.py promotion
```

Open the read-only local learn-once dashboard:

```bash
python -X utf8 chainseer_solana.py dashboard
```

Then visit [http://127.0.0.1:8767/](http://127.0.0.1:8767/). The dashboard
refreshes every ten seconds from persisted state under `solana_learning/`. It
shows tri-state evidence quality, Jupiter/RPC coverage, credential-safe
per-method infrastructure health, recovery-queue state, shadow performance,
promotion blockers, hash-linked
ledger verification, and the dedicated Solana Timechain head. It never exposes
API-key values or RPC URL paths/query strings, and it has no mutating controls.

## Automatic learn-once

Install and immediately start the paper-only scheduled learner from Git Bash:

```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass \
  -File ./manage_chainseer_solana_learning_task.ps1 install \
  -IntervalMinutes 10
```

It repeats every ten minutes while the Windows user is logged on. Overlapping
cycles are ignored by Task Scheduler and independently blocked by a named
process mutex. It continues until manually stopped:

Change the cadence without forcing an immediate run:

```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass \
  -File ./manage_chainseer_solana_learning_task.ps1 reschedule \
  -IntervalMinutes 1
```

One-minute scheduling can consume a free RPC allowance quickly. Monitor
per-method attempts, retries, and failures in the dashboard and daily log.

```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass \
  -File ./manage_chainseer_solana_learning_task.ps1 stop
```

Inspect or resume it:

```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass \
  -File ./manage_chainseer_solana_learning_task.ps1 status

powershell.exe -NoProfile -ExecutionPolicy Bypass \
  -File ./manage_chainseer_solana_learning_task.ps1 start
```

Tail today's execution log:

```bash
tail -n 30 "./solana_learning/logs/learn-once-$(date +%Y-%m-%d).log"
```

The schedule, current/last cycle state, access mode, and cycle freshness also
appear on the dashboard. Stopping the task preserves all catalogs, analysis
history, shadow state, hash-linked ledgers, logs, and Timechain rings.

Verify both evidence ledgers, policy-bound state, and the Solana Timechain:

```bash
python -X utf8 chainseer_solana.py verify
```

Runtime state is isolated under `solana_learning/`. Cognitive evidence is
isolated under `solana_chain/`.

## Evidence states

- `complete_safe`: every observation required for the token's current cohort
  completed and no configured token hard stop fired. This is not, by itself,
  permission to enter a paper position.
- `complete_unsafe`: required evidence completed and a token-specific hard stop
  fired.
- `infrastructure_indeterminate`: RPC, DexScreener, or Jupiter evidence was unavailable.
  This is never mislabeled as token risk and never permits a shadow entry.

Admission is a separate state machine. Pre-graduation tokens remain
`graduation_pending`; completed curves wait for `canonical_migration_pending`
and `market_indexing_pending` gates as needed. Only
`graduated_market_ready` can pass the shadow-entry gate.

## Promotion boundary

The default promotion evaluator requires all of the following:

- at least 200 unique token observations;
- at least 50 closed shadow positions;
- positive aggregate net return;
- positive return after removing the best position;
- at least 5 profitable positions and a 15% winner rate;
- maximum modeled drawdown no greater than 35%;
- at least 90% two-way quote coverage;
- at least 90% unsigned transaction-assembly coverage;
- infrastructure-indeterminate observations no higher than 10%;
- verified observation and shadow ledgers.

Passing these gates produces `PROMOTABLE_FOR_REVIEW`, not live trading.
Automatic live enablement remains false and transaction broadcast is absent.
