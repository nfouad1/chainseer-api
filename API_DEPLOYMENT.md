# Chainseer API deployment

The first production deployment targets a single Render web service in
Frankfurt with a persistent disk mounted at `/data`.

## Why one instance

Every completed analysis appends to one Timechain head. The API therefore runs
one worker, holds a filesystem lease, and deliberately stays at one service
instance. Do not enable horizontal scaling until Chainseer has a coordinated
multi-writer Timechain design.

The optional confirmed-block watcher runs inside that same worker. Never start
`chainseer_controls.py watch run` as a second process against the production
root; the shared `.chainseer-api.lock` refuses competing writers.

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

The Blueprint enables the watcher every 15 seconds with two confirmations.
It remains idle until an authenticated `POST /v1/watch` adds a token. Use
authenticated `GET /v1/watch` to inspect subscriptions and the latest cycle,
and `DELETE /v1/watch/<address>` to remove one.

## Timechain durability

The container image pins Cypher Tempre to a specific source commit. The image
contains the runtime only; it never contains local Chainseer memory. Production
creates its own Genesis Ring on the persistent disk. Render's disk preserves
the rings and blockspace across service deploys and restarts.

Before each release:

1. Download a backup of `/data/chainseer_chain`.
2. Run `timechain.py verify` against the backup.
3. Deploy one instance.
4. Confirm `/health/ready`.
5. Run a known-contract analysis and confirm its ring index and hash.

## Domain

The API can initially use its Render hostname. After the web launch is stable,
attach `api.usechainseer.com` in Render and add the CNAME record Render provides
in STRATO. `usechainseer.com` and `www.usechainseer.com` remain assigned to the
website host.
