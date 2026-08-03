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

## Fly.io setup

1. Install `flyctl`, sign in, and keep the API source in a private GitHub
   repository.
2. Treat `fly.toml` as the authoritative deployment configuration. It defines
   the `chainseer-api` application, Frankfurt region, Docker build, singleton
   machine, `/data` mount, health check, and watcher settings.
3. Confirm the persistent volume exists:

   ```bash
   fly volumes list -a chainseer-api
   ```

   If it is absent, create it before the first deployment:

   ```bash
   fly volumes create chainseer_timechain --region fra --size 5 -a chainseer-api
   ```

4. Generate a random `CHAINSEER_API_TOKEN` of at least 32 characters, store it
   in a password manager, and configure it with the dedicated Base and Solana
   RPC endpoints:

   ```bash
   fly secrets set -a chainseer-api \
     CHAINSEER_API_TOKEN=<secret> \
     CHAINSEER_BASE_RPC_URL=<https-url> \
     CHAINSEER_SOLANA_RPC_URL=<https-url>
   ```

   `JUPITER_API_KEY` is optional and should also be configured as a Fly secret
   when authenticated Jupiter capacity is available.
5. Deploy with `fly deploy -a chainseer-api`.
6. Wait for `https://chainseer-api.fly.dev/health/ready` to return HTTP 200 and
   list `robinhood`, `base`, and `solana`. Confirm
   `base_rpc_configured=true` and `benchmark_capture.state=ready`.
7. Store `https://chainseer-api.fly.dev` as `CHAINSEER_API_URL` in the
   Chainseer Sites environment.
8. Store the same API token as the server-side `CHAINSEER_API_TOKEN` secret in
   Sites.
9. Redeploy the website, then run one scanner smoke test per network.

`fly.toml` enables the watcher at a 60-second poll interval with two block
confirmations. Watch subscriptions are authenticated:

- `GET /v1/watch`
- `POST /v1/watch` with `{"address":"0x..."}`
- `DELETE /v1/watch/{address}`

These endpoints only manage monitoring. The production API has no private-key,
transaction-signing, or broadcast capability.

## Automatic benchmark capture

`fly.toml` enables benchmark capture under `/data/benchmark`, which is on
the same persistent disk as the production Timechain but outside its root.
Every fresh successful analysis appends one immutable prediction observation.
Cache hits are not captured again.

Fly uses the explicit `CHAINSEER_BENCHMARK_ANALYZER_VERSION` value in
`fly.toml`; update it for each production release so observations remain tied
to a declared deployed source revision. Token addresses are assigned
deterministically to `train` (60%), `validation` (20%), or `test` (20%); every
observation for the same token stays in the same split.

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
persistent Fly volume. The volume preserves rings, blockspace, senses,
modalities, and safely grown faculties across machine deploys and restarts. A
partial or epoch-mismatched registry fails startup instead of being silently
repaired.

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

The API can use `https://chainseer-api.fly.dev` directly. To attach the custom
API hostname, run `fly certs add api.usechainseer.com -a chainseer-api`, inspect
the required DNS records with
`fly certs setup api.usechainseer.com -a chainseer-api`, and add those records
in STRATO. `usechainseer.com` and `www.usechainseer.com` remain assigned to the
website host.
