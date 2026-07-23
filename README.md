# Chainseer API

Private production service for the Chainseer on-chain analysis website.

Chainseer analyzes Robinhood Chain token contracts using block-pinned RPC
queries, contract-source verification, liquidity and holder evidence,
cross-source consistency checks, and an investor-oriented risk model.
Completed analyses pass through a non-bypassable Cypher Tempre cognitive loop:
the agent recalls relevant rings, fires executable senses and modalities,
screens the structured analysis through its covenant membrane, passes Proof of
Qualia, and appends tamper-evident analysis and cognitive-completion rings.
When a genuine capability gap is encountered, Cambium may grow a safe
primitive faculty and seal an epoch hash over the persistent registry.

## Production architecture

- FastAPI HTTP boundary with bearer authentication and per-client rate limits
- one bounded analysis queue and one worker
- one process lease protecting the Timechain head
- one Render instance in Frankfurt
- persistent disk mounted at `/data`
- Cypher Tempre v3.28.0 pinned to an exact source commit in the container image
- persistent senses/modalities under `/data/chainseer_chain/registry`
- fail-closed epoch verification for faculty-registry integrity
- cognition receives trusted structured fields only, never raw upstream bodies
- cognitive learning cannot alter the deterministic token score or lower risk
- public responses omit raw upstream responses and internal query parameters

The service must not be horizontally scaled while it uses a single filesystem
Timechain. Multiple writers would need a coordinated consensus design.

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

Never commit API tokens, environment files, local Timechain rings, learning
databases, or analysis reports.
