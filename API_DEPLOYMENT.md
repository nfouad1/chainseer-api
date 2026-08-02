# Chainseer API deployment

The production deployment targets a single Fly.io application machine in
Frankfurt with a persistent disk mounted at `/data`.

## Why one instance

Every completed analysis appends to one Timechain head. The API therefore runs
one worker, holds a filesystem lease, and deliberately stays at one service
instance. The confirmed-block watcher is scheduled inside that same process,
so analyses, watch transitions, outcomes, and calibration evidence all share
one ordered writer. Do not run the standalone watcher beside the API or enable
horizontal scaling until Chainseer has a coordinated multi-writer Timechain
design.

## Render setup

1. Keep the API source in a private GitHub repository.
2. In Render, choose **New > Blueprint** and connect that repository.
3. Render reads `render.yaml`. Confirm:
   - service: `chainseer-api`
   - region: Frankfurt
   - plan: Starter or larger
   - instances: 1
   - disk: 5 GB mounted at `/data`
4. When prompted for `CHAINSEER_API_TOKEN`, generate a random value of at least
   32 characters and store it in a password manager. Never commit it.
5. Set `CHAINSEER_BASE_RPC_URL` and `CHAINSEER_SOLANA_RPC_URL` to dedicated
   HTTPS RPC endpoints
   before sustained public use. The Blueprint's public mainnet endpoint is
   suitable for a bounded smoke test but can rate-limit production traffic.
   `JUPITER_API_KEY` is optional; set it in Render when authenticated Jupiter
   capacity is available.
6. Deploy and wait for `/health/ready` to return HTTP 200 and list
   `robinhood`, `base`, and `solana`. Confirm
   `base_rpc_configured=true` and `benchmark_capture.state=ready`.
7. Copy the public `https://<service>.onrender.com` URL.
8. Store that URL as `CHAINSEER_API_URL` in the Chainseer Sites environment.
9. Store the same API token as the secret `CHAINSEER_API_TOKEN` in Sites.
10. Redeploy the website, then run one scanner smoke test per network.

The Blueprint enables the watcher at a 15-second poll interval with two block
confirmations. Watch subscriptions are authenticated:

- `GET /v1/watch`
- `POST /v1/watch` with `{"address":"0x..."}`
- `DELETE /v1/watch/{address}`

These endpoints only manage monitoring. The production API has no private-key,
transaction-signing, or broadcast capability.

## Automatic benchmark capture

The Blueprint enables benchmark capture under `/data/benchmark`, which is on
the same persistent disk as the production Timechain but outside its root.
Every fresh successful analysis appends one immutable prediction observation.
Cache hits are not captured again.

The analyzer version comes from Render's runtime `RENDER_GIT_COMMIT`, so each
observation is tied to the exact deployed source revision. Token addresses are
assigned deterministically to `train` (60%), `validation` (20%), or `test`
(20%); every observation for the same token stays in the same split.

The API job response exposes `benchmark_capture` with the case ID, observation
hash, split, cohort, and analyzer version. `/health/ready` exposes aggregate
capture state and ledger hashes without exposing scanned token addresses.
Storage or validation failures set capture state to `degraded` but do not
discard an otherwise successful analysis.

Automatic capture does not assign outcomes. Reviewers append those later with
the `chainseer_benchmark.py label` command described in `BENCHMARK.md`. Back up
`/data/benchmark` together with `/data/chainseer_chain`.

## Timechain durability

The container image pins Cypher Tempre to a specific source commit. The image
contains the runtime only; it never contains local Chainseer memory. Production
creates its own Genesis Ring and a bootstrap faculty-registry epoch on the
persistent disk. Render's disk preserves rings, blockspace, senses, modalities,
and safely grown faculties across service deploys and restarts. A partial or
epoch-mismatched registry fails startup instead of being silently repaired.

The image also carries the reviewed faculty pack at
`/app/faculties/chainseer-production-v1.json`. On startup,
`CHAINSEER_FACULTY_PACK_PATH` causes Chainseer to verify its canonical hash,
screen it through the covenant membrane, import any missing definitions before
Recall caches the registry, and seal the changed registry epoch. Subsequent
restarts verify the existing definitions without producing duplicate import
rings. `/health/ready` exposes the bounded `faculty_pack` status and pack hash.

Before each release:

1. Download backups of `/data/chainseer_chain` and `/data/benchmark`.
2. Run `timechain.py verify` against the backup.
3. Deploy one instance.
4. Confirm `/health/ready`.
5. Run a known-contract analysis and confirm its ring index and hash.
6. Confirm the public result contains `timechain.cognition.status=complete`
   and a `timechain.cognitive_ring`.
7. Confirm `/health/ready` reports the watcher enabled and no watcher error.
8. Confirm benchmark capture is `ready` and a fresh known-contract scan
   increments its observation count exactly once.
9. Confirm the public result contains `entity_graph.graph_hash`, the graph
   anchor matches the analysis block/slot boundary, and graph summary
   `changes_legitimacy_score=false`.

## Domain

The API can initially use its Render hostname. After the web launch is stable,
attach `api.usechainseer.com` in Render and add the CNAME record Render provides
in STRATO. `usechainseer.com` and `www.usechainseer.com` remain assigned to the
website host.
