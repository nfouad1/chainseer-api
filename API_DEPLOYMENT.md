# Chainseer API deployment

The first production deployment targets a single Render web service in
Frankfurt with a persistent disk mounted at `/data`.

## Why one instance

Every completed analysis appends to one Timechain head. The API therefore runs
one worker, holds a filesystem lease, and deliberately stays at one service
instance. Do not enable horizontal scaling until Chainseer has a coordinated
multi-writer Timechain design.

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
5. Deploy and wait for `/health/ready` to return HTTP 200.
6. Copy the public `https://<service>.onrender.com` URL.
7. Store that URL as `CHAINSEER_API_URL` in the Chainseer Sites environment.
8. Store the same API token as the secret `CHAINSEER_API_TOKEN` in Sites.
9. Redeploy the website, then run one public scanner smoke test.

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

## Domain

The API can initially use its Render hostname. After the web launch is stable,
attach `api.usechainseer.com` in Render and add the CNAME record Render provides
in STRATO. `usechainseer.com` and `www.usechainseer.com` remain assigned to the
website host.
