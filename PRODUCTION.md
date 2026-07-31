# Chainseer production runbook

Chainseer has two deployable surfaces:

- `chainseer_api.py`: the private Python analysis service.
- `chainseer_web/`: the public website and server-side API proxy.

The browser never receives the Python service token. The website proxy submits
and polls jobs, while the Python service serializes all analysis and Timechain
writes through one worker.

## Safety boundaries

- Run exactly one API process and one Uvicorn worker per Timechain root.
- Mount the Timechain root on a persistent disk.
- Mount the Cypher Tempre skill read-only and set `CHAINSEER_SKILL_DIR`.
- Never expose the Python API without `CHAINSEER_API_TOKEN`.
- Use a random token of at least 32 characters in production.
- Back up the Timechain root and its evidence directory before upgrades.
- Do not run `learn_once` against the same Timechain root while the API is live.
- The website contains no wallet connection or transaction execution.
- Agent chat is intentionally not included in this release.

The API also obtains an OS-level exclusive lease on its Timechain root. A
second process will refuse to start instead of risking concurrent ring writes.

## Local production rehearsal

The simplest local review command starts both the authenticated API and the
website with one ephemeral shared token:

```powershell
.\manage_chainseer_local.ps1 start
```

Useful companion commands:

```powershell
.\manage_chainseer_local.ps1 status
.\manage_chainseer_local.ps1 stop
```

For manual operation, create local environment files from `.env.api.example`
and `chainseer_web/.env.example`. Both must contain the same API token.

Start the API:

```powershell
$env:PYTHONUTF8="1"
python -X utf8 chainseer_api.py
```

Start the website in a second terminal:

```powershell
cd chainseer_web
npm run dev
```

Check:

- `http://127.0.0.1:8000/health/live`
- `http://127.0.0.1:8000/health/ready`
- `http://localhost:3000`

## API production deployment

`Dockerfile.api` is suitable for a single-replica container service with a
persistent disk mounted at `/data`. Mount the installed Cypher Tempre skill at
`/opt/cypher-tempre-self-model:ro`.

Required environment:

```text
CHAINSEER_ENVIRONMENT=production
CHAINSEER_API_TOKEN=<random 32+ character secret>
CHAINSEER_ALLOWED_ORIGINS=https://usechainseer.com
CHAINSEER_ALLOWED_HOSTS=api.usechainseer.com,127.0.0.1
CHAINSEER_CHAIN_ROOT=/data/chainseer_chain
CHAINSEER_SKILL_DIR=/opt/cypher-tempre-self-model
```

Use a service that supports a persistent volume and graceful shutdown of at
least 180 seconds. Do not enable autoscaling or multiple replicas against the
same Timechain. Put TLS and request-size limits in front of the container.
Analysis jobs and the short response cache are intentionally process-local;
completed analyses remain durable in the Timechain. If the API restarts while
a browser is polling, the browser can safely resubmit the address.

## Website production deployment

Configure these server-side runtime values in the website host:

```text
CHAINSEER_API_URL=https://<private-api-host>
CHAINSEER_API_TOKEN=<same secret as the API>
```

The token is used only by the server-side proxy. It must never use a
`NEXT_PUBLIC_` prefix.

After deployment:

1. Confirm security headers on the homepage and `/api/analyses`.
2. Submit a known contract and wait for a sealed report.
3. Verify the displayed Ring and block pin against the local Timechain.
4. Confirm invalid addresses return 422 and excessive submissions return 429.
5. Confirm a source outage produces an explicit failure or unknown state.
6. Confirm a second API instance refuses to acquire the Timechain lease.

## Rollback and incident response

- Website: redeploy the previous saved site version.
- API: stop traffic, preserve `/data`, deploy the previous image, then verify
  the Timechain before reopening traffic.
- Integrity failure: keep the API offline and use the Cypher Tempre verification
  and immune recovery tools. Never edit the ring ledger manually.
