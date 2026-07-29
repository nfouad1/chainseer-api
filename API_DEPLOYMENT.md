# Chainseer API deployment

The first production deployment targets a single Render web service in
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
5. Set `CHAINSEER_SOLANA_RPC_URL` to a dedicated HTTPS Solana RPC endpoint
   before sustained public use. The Blueprint's public mainnet endpoint is
   suitable for a bounded smoke test but can rate-limit production traffic.
   `JUPITER_API_KEY` is optional; set it in Render when authenticated Jupiter
   capacity is available.
6. Deploy and wait for `/health/ready` to return HTTP 200 and list both
   `robinhood` and `solana`.
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

## Timechain durability

The container image pins Cypher Tempre to a specific source commit. The image
contains the runtime only; it never contains local Chainseer memory. Production
creates its own Genesis Ring and a bootstrap faculty-registry epoch on the
persistent disk. Render's disk preserves rings, blockspace, senses, modalities,
and safely grown faculties across service deploys and restarts. A partial or
epoch-mismatched registry fails startup instead of being silently repaired.

Before each release:

1. Download a backup of `/data/chainseer_chain`.
2. Run `timechain.py verify` against the backup.
3. Deploy one instance.
4. Confirm `/health/ready`.
5. Run a known-contract analysis and confirm its ring index and hash.
6. Confirm the public result contains `timechain.cognition.status=complete`
   and a `timechain.cognitive_ring`.
7. Confirm `/health/ready` reports the watcher enabled and no watcher error.

## Domain

The API can initially use its Render hostname. After the web launch is stable,
attach `api.usechainseer.com` in Render and add the CNAME record Render provides
in STRATO. `usechainseer.com` and `www.usechainseer.com` remain assigned to the
website host.
