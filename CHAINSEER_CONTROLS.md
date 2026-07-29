# Chainseer monitoring and execution controls

`chainseer_controls.py` adds deliberately separated monitoring and execution
controls to the Robinhood Chain and Solana analyzers:

1. confirmed-block monitoring and state-drift alerts;
2. automated outcome collection with tighten-only calibration proposals;
3. a short-lived, one-time `TradePermit` boundary;
4. separate MEV, cross-chain, and low-trust social/KOL evidence.

It does **not** contain a wallet, private key, transaction signer, or broadcast
method. A TradePermit is an authorization artifact, not a transaction.

## Safety model

- RPC state is re-read at a confirmed `block_pin`.
- Mutable HTTP evidence is timestamped and content-addressed; it is not
  represented as historical block state.
- The watcher scans ownership/upgrade, LP-burn, and token-transfer logs, then
  performs a full analysis only after a meaningful change, a holder refresh
  window, an outcome horizon, or the maximum refresh interval.
- The Solana watcher incrementally indexes confirmed mint signatures and
  compares compact snapshots of the token program, mint/freeze authorities,
  extensions, supply, largest accounts, primary market, liquidity, and price.
  Routine signatures and small account rotations remain observational; they
  do not cause an expensive full analysis.
- Material Solana changes trigger a full analysis after a short debounce.
  Authority, program, supply, extension, or market-identity changes bypass the
  debounce. A periodic confirmed-state reconciliation catches missed provider
  events.
- Optional holder, market, and signature-provider failures are recorded as
  `infrastructure_indeterminate` and retain the last confirmed observation;
  they never become a fabricated token state change. Mint-account and supply
  failures remain fail-closed for that watcher cycle.
- Reorgs are detected by comparing the previously processed block hash.
- Analyses, watcher transitions, outcomes, calibration adoption, permit issue,
  and permit consumption are sealed into the Timechain.
- The watcher and API share `.chainseer-api.lock`; only one process may append
  to a Timechain root.
- Outcome labels are separated into security correctness and market
  performance. Correlated refreshes do not all become calibration baselines.
- Calibration proposals can only tighten the pre-trade policy. Adoption is a
  separate, PoQ-gated command.
- Social/KOL and cross-chain context cannot trigger a green safety verdict or
  override a hard stop.
- MEV is evaluated from the executable quote and affects only execution
  authorization, not the token legitimacy score.

## Watch tokens

The authenticated production API accepts either network:

```bash
curl -X POST "https://api.usechainseer.com/v1/watch" \
  -H "Authorization: Bearer $CHAINSEER_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"network":"solana","address":"YourSolanaMint"}'
```

Solana cursors and alerts are stored separately:

```text
chainseer_chain/controls/solana_watcher_state.json
chainseer_chain/controls/solana_watcher_alerts.jsonl
```

Each material alert carries the confirmed slot, compact event evidence, the
fresh analysis ring, a content hash, and a `solana_watch_transition` Timechain
ring. The watcher remains observation-only: it cannot sign or broadcast a
transaction.

The standalone CLI below continues to manage Robinhood Chain subscriptions.

Global options must precede the command:

```bash
python -X utf8 chainseer_controls.py \
  --chain-root ./chainseer_chain \
  watch add 0xYourTokenAddress
```

Run one confirmed-block cycle:

```bash
python -X utf8 chainseer_controls.py \
  --chain-root ./chainseer_chain \
  watch once
```

Run continuously until `Ctrl+C`:

```bash
python -X utf8 chainseer_controls.py \
  --chain-root ./chainseer_chain \
  watch run --poll-seconds 3 --confirmations 2
```

Inspect or remove subscriptions:

```bash
python -X utf8 chainseer_controls.py --chain-root ./chainseer_chain watch status
python -X utf8 chainseer_controls.py --chain-root ./chainseer_chain watch remove 0xYourTokenAddress
```

State and alerts are stored under:

```text
chainseer_chain/controls/watcher_state.json
chainseer_chain/controls/watcher_alerts.jsonl
```

Do not run the watcher against the production API's Timechain in a second
process. The shared lease refuses this configuration. A production scheduler
must pause the API or run monitoring inside the same single-writer service.

## Outcome calibration

The watcher collects append-only outcomes at 1 hour, 6 hours, 24 hours, 7 days,
and 30 days. It automatically refreshes:

```text
chainseer_chain/controls/calibration_proposal.json
```

View metrics and the adopted policy:

```bash
python -X utf8 chainseer_controls.py \
  --chain-root ./chainseer_chain \
  calibration status
```

Create a fresh proposal:

```bash
python -X utf8 chainseer_controls.py \
  --chain-root ./chainseer_chain \
  calibration propose
```

Adopt a proposal only after reviewing its sample size and false-negative rate:

```bash
python -X utf8 chainseer_controls.py \
  --chain-root ./chainseer_chain \
  calibration adopt ./chainseer_chain/controls/calibration_proposal.json
```

Insufficient data never changes policy. Adoption cannot reduce the minimum
trade score or broaden the allowed risk levels.

## TradePermit

The quote input must be an executable route observation:

```json
{
  "observed_block": 123456,
  "pair_address": "0xCanonicalPair",
  "amount_in": "1000000000000000000",
  "amount_out": "900000000000000000",
  "min_amount_out": "850000000000000000",
  "price_impact_bps": 75,
  "slippage_bps": 150,
  "route": ["0xCanonicalPair"],
  "source": "router-quote-v1"
}
```

Create a permit:

```bash
python -X utf8 chainseer_controls.py \
  --chain-root ./chainseer_chain \
  permit create 0xToken \
  --amount-in 1000000000000000000 \
  --recipient 0xRecipient \
  --quote quote.json \
  --private-routing
```

The guard runs a new confirmed-block analysis and refuses authorization when:

- any analysis hard stop is active;
- risk or score violates the adopted calibration policy;
- confidence is limited;
- the quote is stale, from the wrong pool, or for the wrong amount;
- output, slippage, or price-impact checks fail;
- PoQ refuses the permit.

An execution adapter must atomically verify and consume the permit immediately
before signing:

```bash
python -X utf8 chainseer_controls.py \
  --chain-root ./chainseer_chain \
  permit verify permit.json --consume
```

A consumed, expired, modified, wrong-chain, or block-stale permit is invalid.
Permit verification still does not sign or broadcast anything.

## Evidence adapters

`Chainseer` accepts optional `cross_chain_provider` and `social_kol_provider`
callables. Each receives `(token_address, block_pin)` and must return a list of
records. Adapter responses are recorded in the provenance ledger.

Cross-chain records remain `provider_attested` unless both chain transactions
are independently replayed. A same-address DexScreener market on another chain
is surfaced as market context only and is not treated as bridged fund flow.

Social/KOL records remain low-trust context with a bounded score. They cannot
create a hard stop or override on-chain risk evidence.
