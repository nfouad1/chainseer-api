# Chainseer API

Private production service for the Chainseer on-chain analysis website.

Chainseer analyzes Robinhood Chain token contracts and Solana SPL mints.
Robinhood analyses use block-pinned RPC queries, contract-source verification,
liquidity and holder evidence, and cross-source consistency checks. Solana
analyses use confirmed mint controls and supply, largest-account
concentration, DexScreener markets, and bounded two-way Jupiter route evidence.
Both produce an investor-oriented risk model with explicit unknowns.
Completed analyses pass through a non-bypassable Cypher Tempre cognitive loop:
the agent recalls relevant rings, fires executable senses and modalities,
screens the structured analysis through its covenant membrane, passes Proof of
Qualia, and appends tamper-evident analysis and cognitive-completion rings.
When a genuine capability gap is encountered, Cambium may grow a safe
primitive faculty and seal an epoch hash over the persistent registry.

## Production architecture

- FastAPI HTTP boundary with bearer authentication and per-client rate limits
- one bounded analysis queue and one worker
- a confirmed-block watcher in the same single-writer process
- event-triggered rescans for ownership, proxy, transfer, and LP-burn drift
- one-time, block-bound TradePermits with mandatory executable-quote MEV checks
- append-only security and market outcomes with tighten-only calibration proposals
- bounded social/KOL context and provider-attested cross-chain-flow adapters
- one process lease protecting the Timechain head
- one Render instance in Frankfurt
- persistent disk mounted at `/data`
- Cypher Tempre v3.28.0 pinned to an exact source commit in the container image
- persistent senses/modalities under `/data/chainseer_chain/registry`
- fail-closed epoch verification for faculty-registry integrity
- cognition receives trusted structured fields only, never raw upstream bodies
- cognitive learning cannot alter the deterministic token score or lower risk
- public responses omit raw upstream responses and internal query parameters
- network-aware jobs and caches prevent Robinhood and Solana results colliding
- Solana reports distinguish a confirmed starting-slot anchor from an EVM
  block pin; each later RPC/HTTP observation is content-hashed

The service must not be horizontally scaled while it uses a single filesystem
Timechain. Multiple writers would need a coordinated consensus design.

## Analysis request

`POST /v1/analyses` accepts either network:

```json
{"network":"robinhood","address":"0x..."}
```

```json
{"network":"solana","address":"So11111111111111111111111111111111111111112"}
```

Solana is intentionally conservative. Generic SPL scans do not claim creator
attribution, LP-withdrawal custody, or wash-trading detection unless those
facts are independently verified. Largest-account concentration can include
AMM and program vaults, so it is cautionary evidence rather than a hard stop.
The API is analysis-only and contains no signing or transaction-broadcast path.

## Local checks

```bash
python -X utf8 -m pip install -r requirements-api.txt
python -X utf8 -m unittest discover -s tests -v
```

The tests require `CHAINSEER_SKILL_DIR` to point to an installed Cypher Tempre
skill directory.

## Deployment

Render reads [`render.yaml`](render.yaml) and builds
[`Dockerfile.api`](Dockerfile.api). Follow
[`API_DEPLOYMENT.md`](API_DEPLOYMENT.md) for secrets, health checks, website
integration, backups, and domain configuration.

See [`CHAINSEER_CONTROLS.md`](CHAINSEER_CONTROLS.md) for watcher, calibration,
TradePermit, MEV, social/KOL, and cross-chain adapter operation.

See [`BENCHMARK.md`](BENCHMARK.md) for the versioned, time-separated benchmark
schema and deterministic evaluation workflow. Benchmark v1 keeps token outcomes
separate from infrastructure failures and emits hashes suitable for Timechain
sealing after PoQ review.

Never commit API tokens, environment files, local Timechain rings, learning
databases, or analysis reports.
