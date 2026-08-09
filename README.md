# Chainseer

On-chain risk analysis that watches tokens autonomously, records every outcome against a tamper-evident ledger, and never signs a transaction.

## What it does

Chainseer monitors token launches on Solana (Pump.fun and Meteora DBC), Robinhood Chain, and Base. For every token it sees, it gathers on-chain evidence — holder concentration, liquidity custody, creator history, market quality — and produces a risk report with hard stops. Hard stops are non-negotiable: if a token fails one, the analysis says so regardless of how good everything else looks.

On Solana, it does more than respond to requests. It runs autonomous learning cycles that sample the Pump.fun and Meteora programs for new launches, track them through graduation, evaluate them against risk gates, and paper-trade shadow positions (0.01 SOL each) to measure whether the scoring system is actually predicting outcomes. Every cycle appends to a hash-chained ledger so the history can't be altered.

**It never signs transactions. It never holds capital. There is no wallet.**

## Honest status

Chainseer is research infrastructure under active development. Three limitations are load-bearing enough to state up front:

**The score does not yet discriminate.** Most entry-eligible tokens score 90–95, and winners and losers score alike. The scoring formula is not currently a useful predictor of which entries will be profitable.

**Paper-trading results are poor.** Across 43 closed shadow positions: 7% win rate (3 profitable), −71.3% net return, −81.1% excluding the single best position. These are paper results on 0.01 SOL notional, and they are the measurement the shadow portfolio exists to produce. They do not currently support a claim that the system picks winners.

**Discovery samples rather than sweeps.** A Pump.fun sweep reaches roughly six seconds of chain per five-minute cycle — about 2% coverage — because the program's transaction volume vastly exceeds the per-cycle page budget. Meteora DBC reaches further but also falls behind. Launches outside a sweep's reach are recorded as a deferred backlog and drained on later cycles, but tokens can be, and are, missed.

The learning loop described below is built and instrumented. **It has not yet closed:** no calibration proposal has been generated, and 43 closed positions is far too few to fit scoring weights without overfitting.

## Two modes

### Request-response API

Submit a token address, get a structured risk report:

```
POST /v1/analyses  {"network": "solana", "address": "3fqify...Dmpump"}
```

The API is authenticated, rate-limited, and runs on a single Fly.io instance in Frankfurt. Reports include risk level, legitimacy score, hard stops, confidence, holder evidence, market data, and a tamper-evident cognitive provenance trace.

Supported networks:

| Network | Analysis focus | Status |
|---|---|---|
| **Solana** | SPL mint authority, holder concentration, Jupiter route quality, DexScreener market evidence, Pump.fun graduation state | available |
| **Robinhood Chain** | Bytecode risk, proxy ownership, LP custody, honeypot detection, deployer history, sell restrictions | **unavailable on the hosted API** — the Robinhood RPC returns HTTP 403 to the deployment's egress IPs. Works from local/self-hosted deployments whose network is not blocked. |
| **Base** | Same EVM evidence core as Robinhood, chain-isolated (chain ID 8453, Base RPC, Base Blockscout) | available |

### Autonomous Solana learning engine

The Solana engine runs continuous cycles that:

1. **Discover** — sample the Pump.fun and Meteora programs for new CreateEvents via on-chain signatures, recording what each sweep could not reach
2. **Catalog** — store every launch in a local index with bonding curve, creator, and timestamp
3. **Evaluate** — run the risk analyzer on a budget of tokens per cycle (graduated candidates first, then raw launches, then previously-unanalysed backlog)
4. **Enter** — if a token passes every gate (evidence complete, no hard stops, graduated, minimum age), open a paper shadow position
5. **Mark** — re-price open positions each cycle, exit on stop-loss (0.65x), take-profit (3.0x), maximum hold (6h), or risk signal deterioration
6. **Record** — append win/loss outcomes, score distributions, and failure modes for calibration against realized results

The shadow portfolio uses 0.01 SOL per position. It is purely observational — no real capital, no execution path, no slippage risk. But the positions are tracked against real market prices via Jupiter quotes, so the outcomes reflect actual market conditions.

## How the Solana pipeline works

```
New Pump.fun launch
        │
        ▼
┌─ Discovery ──────────────────────────────────────┐
│  Sample Pump.fun program signatures               │
│  Decode CreateEvents → catalog with mint,         │
│  bonding curve, creator, timestamp                │
│  Unreached span → deferred backlog, drained       │
│  on later cycles rather than discarded            │
└────────────────────┬──────────────────────────────┘
                     │
                     ▼
┌─ Admission Cascade ───────────────────────────────┐
│  infrastructure_indeterminate                     │
│       → graduation_pending                       │
│       → canonical_migration_pending               │
│       → market_indexing_pending                   │
│       → execution_evidence_pending                │
│       → distribution_pending                      │
│       → market_age_pending                         │
│       → graduated_market_ready  ← entry gate       │
│                                                    │
│  Hard stops at any stage → graduated_market_unsafe │
│  Missing data → infrastructure_indeterminate       │
└────────────────────┬──────────────────────────────┘
                     │
                     ▼
┌─ Entry Gate ─────────────────────────────────────┐
│  evidence_state == complete_safe                   │
│  AND admission_state == graduated_market_ready     │
│  AND age_seconds >= minimum_age_seconds            │
│  AND NOT momentum_entry_blocked                    │
└────────────────────┬──────────────────────────────┘
                     │ pass
                     ▼
              Shadow position opened
              (0.01 SOL, paper-only)
```

### What triggers a hard stop

- **Holder concentration**: top 1 holder > 25% of supply, or top 10 > 90%
- **Creator risk**: industrialized deployment (10+ launches in 24h), scam-flagged wallets
- **Market quality**: Jupiter roundtrip retention too low, buy price impact too high
- **Liquidity**: insufficient liquidity or unresolved concentration state

### What triggers an exit

Priority order:
1. **Risk signal** — re-analysis detects hard stops on a held position
2. **Stop loss** — position drops below 0.65x entry
3. **Take profit** — position exceeds 3.0x entry
4. **Maximum hold** — position held for 6 hours regardless of P&L

## Evidence model

Chainseer treats missing data differently from negative data. If an RPC call fails or a provider is down, the token gets classified as `infrastructure_indeterminate` — not unsafe, not safe, just unknown. This prevents a Solana RPC outage from silently flagging every token as risky.

The scoring formula separates hard stops from warnings from scoring signals. Hard stops are binary — present or absent. Warnings are informational. The composite score deducts from 100 for each hard stop, warning, and infrastructure error, and is capped at 50 for tokens in `distribution_pending` state.

**Known limitation**: the current formula produces near-zero discrimination among entry-eligible tokens (most score 90–95), which means the score alone doesn't predict which entries will be profitable. The h6 buy/sell ratio and Jupiter roundtrip retention are the strongest outcome discriminators observed so far, but are weighted as minor warnings rather than scoring terms. This is a known calibration gap under active development.

## Calibration and learning

The pieces required to calibrate scoring against realized outcomes are built:

- **Outcome recording** — every closed position carries its entry score, evidence state, exit reason, and realized proceeds
- **Time-separated evaluation** — the benchmark keeps each token in one `train` / `validation` / `test` split, and the default evaluation excludes the training split
- **Threshold sensitivity** — the calibration report characterizes the admit rate at every candidate threshold across concentration, retention, and distribution axes
- **A tighten-only calibration engine** — proposals may only make gates stricter, and adoption is human-approved through the governance path

What is not yet done is the join between them. The calibration report labels itself `DESCRIPTIVE_NOT_VALIDATING`: it reports what each threshold *would admit*, never what those admissions *earned*. No calibration proposal has been generated to date. Until that loop closes and enough outcomes accumulate, the scoring weights are hand-set, not learned.

## Timechain

Every analysis, trade event, and learning outcome is appended to a SHA-256 hash-chained ledger called the Timechain. Each ring contains a hash of the previous ring, so any modification, removal, or reordering is detectable by verification.

The Timechain also carries a cognitive trace — a record of what the system's reasoning layer observed, which prior rings it recalled, and whether its cognition was sufficiently grounded in evidence to seal (Proof of Qualia, or PoQ). This is provenance, not a second scoring engine. The deterministic risk checks remain authoritative.

## Architecture

```
┌─────────────────────────────────────────────────┐
│  chainseer_api.py (FastAPI)                      │
│  Auth, rate limits, job queue, caching           │
├─────────────────────────────────────────────────┤
│  chainseer.py              chainseer_solana.py    │
│  EVM analyzer             Solana analyzer        │
│  (Robinhood + Base)       + learning engine       │
├─────────────────────────────────────────────────┤
│  chainseer_controls.py    chainseer_outcome.py   │
│  Monitoring, permits      Outcome ledger         │
├─────────────────────────────────────────────────┤
│  chainseer_entity_graph.py  chainseer_temporal.py│
│  Entity relationships      Time-series risk       │
├─────────────────────────────────────────────────┤
│  Cypher Tempre Timechain (hash-chained ledger)   │
└─────────────────────────────────────────────────┘
```

**Single process. Single writer.** The Timechain is filesystem-backed and cannot be horizontally scaled. There is one Fly.io instance, one analysis worker, one Timechain lease. This is intentional.

## Entity graph

Beyond per-token analysis, Chainseer projects verified entities (deployers, holders, authorities, LP controllers) into a deterministic graph. Exact-address matches across tokens reveal serial deployers, shared LP withdrawal controllers, and authority overlaps. The graph is deliberately conservative — it does not label ordinary large holders as insiders without privileged-link evidence, and it does not cluster wallets by behavioral analysis.

## Safety

- A high score is not a guarantee of safety, liquidity, or future return
- Paper-trading results to date are negative; nothing here demonstrates profitable selection
- Missing evidence lowers confidence — it is never silently converted into a green flag
- The cognitive layer surfaces patterns but cannot override hard stops
- Re-run analysis immediately before any real action — on-chain state changes fast
- This is research and decision-support infrastructure, not financial advice

## Run locally

```bash
# Prerequisites: Python 3.11+, Cypher Tempre self-model skill
pip install -r requirements-api.txt        # API service
pip install -r requirements-solana.txt     # Solana learning engine
pip install -r requirements-test.txt       # test suite only

# CLI analysis
python -X utf8 chainseer.py 0xTokenAddress
python -X utf8 chainseer.py 0xTokenAddress --full

# API
export CHAINSEER_SKILL_DIR="/path/to/cypher-tempre-self-model"
export CHAINSEER_API_TOKEN="your-token"
export CHAINSEER_SOLANA_RPC_URL="https://your-solana-rpc"
python -X utf8 chainseer_api.py
```

Development docs at `http://127.0.0.1:8000/docs`. Interactive docs disabled in production.

Tests run the way CI does:

```bash
python -X utf8 -m unittest discover -s tests
```

## Repository

| File | What it does |
|---|---|
| `chainseer.py` | EVM analyzer — Robinhood Chain and Base |
| `chainseer_base_public.py` | Base chain adapter (chain ID 8453, isolated profile) |
| `chainseer_solana_public.py` | Public Solana analyzer (single-token analysis) |
| `chainseer_solana.py` | Solana learning engine — discovery, evaluation, shadow portfolio, learning cycles |
| `chainseer_api.py` | FastAPI service — auth, queue, cache, Timechain integration |
| `chainseer_controls.py` | Monitoring, TradePermit artifacts, calibration |
| `chainseer_outcome_ledger.py` | Canonical outcome schema and ledger verification |
| `chainseer_entity_graph.py` | Entity relationships, insider-exposure signals |
| `chainseer_temporal_graph.py` | Timechain-derived relationship lifecycle |
| `chainseer_memory.py` | Evidence-citing recall, five-pillar integrity |

## Documentation

| Document | Scope |
|---|---|
| `API_DEPLOYMENT.md` | Fly.io deployment, secrets, health checks |
| `CHAINSEER_CONTROLS.md` | Monitoring, TradePermit safety invariants |
| `CHAINSEER_GOVERNANCE.md` | Faculty governance, calibration proposals, PoQ |
| `BENCHMARK.md` | Time-separated benchmark schema and metrics |
| `ENTITY_GRAPH.md` | Entity graph schema, relationship semantics |
| `TIMECHAIN_MEMORY_CORE.md` | Five-pillar memory architecture |
| `SOLANA_PROTOTYPE.md` | Solana analyzer design decisions |
| `BASE_PROTOTYPE.md` | Base adapter design decisions |
