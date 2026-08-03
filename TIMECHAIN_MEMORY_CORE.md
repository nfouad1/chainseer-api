# Timechain Memory Core

The Timechain Memory Core turns Chainseer’s append-only analysis history into
five linked, verifiable services without making a database or dashboard the
source of truth.

## The five pillars

1. **Timechain Ledger** — immutable analysis, cognitive, governance, alert,
   and outcome history. Registry epochs extend the integrity boundary to the
   active senses, modalities, and governed learned patterns.
2. **Entity Knowledge Graph** — a deterministic temporal projection of
   entities, relationships, first/last observations, relationship lifecycle,
   and risk-score evolution. The projection is disposable and must reproduce
   the same hash when rebuilt from the Timechain.
3. **Pattern & Faculty Store** — candidate → shadow → held-out/outcome
   validation → active lifecycle. Autonomous activation is restricted to
   observability-only or tighten-only effects. A change that might reduce risk
   cannot activate without an exact human override and a new registry epoch;
   signing and broadcast expansion are never overridable.
4. **Outcome Ledger** — real-world security, market, and infrastructure
   outcomes bound to the exact original analysis ring, full ring hash,
   evidence-manifest hash, and block/slot pin. The original forecast is never
   rewritten.
5. **Query & Recall Engine** — authenticated, subject-scoped retrieval of the
   latest assessment, risk history, relationship events, and canonical
   outcomes. Every factual claim carries an exact Ring/evidence citation.
   Uncited claims and legacy analyses whose evidence hash was not sealed in
   the original Ring are excluded.

## Recall API

The API deliberately does not offer raw-ring or free-text search. That would
turn an internal, potentially sensitive cognitive ledger into a data-exposure
surface. The bounded endpoint is:

```http
POST /v1/memory/query
Authorization: Bearer <CHAINSEER_API_TOKEN>
Content-Type: application/json

{
  "network": "base",
  "address": "0x...",
  "topics": ["latest_assessment", "risk_history", "entity_history", "outcomes"],
  "limit": 20
}
```

Each claim contains:

- a deterministic `claim_id` and `claim_hash`;
- its bounded statement and structured value;
- one or more citations containing the exact Ring index, full Ring hash, Ring
  type/time, evidence hash, block/slot anchor, binding state, and claim path;
- a deterministic citation hash.

The complete response is hashed and self-verifies before it leaves the API.
`citation_coverage_pct` is always `100.0`; otherwise the API refuses output.
`GET /v1/memory/citations/{ring}` returns a sanitized proof, never the raw Ring
payload or provider query.

Operational state is exposed through authenticated
`GET /v1/memory/status`. The website accesses it only through a server-side
proxy, so the API token remains secret.

## Backup and recovery model

`chain/` and `registry/` are authoritative. Dashboards, recall responses, and
`temporal_entity_graph-v1.json` are rebuildable. A backup copies the
authoritative paths, hashes every file, verifies the copied Timechain and
registry epoch, and then atomically publishes the manifest-addressed snapshot.

Configure an off-volume backup root in production:

```bash
CHAINSEER_MEMORY_BACKUP_ROOT=/backups/chainseer
```

The path must be outside `CHAINSEER_CHAIN_ROOT`.

Commands:

```bash
python -X utf8 chainseer_memory.py --root ./chainseer_chain status

python -X utf8 chainseer_memory.py --root ./chainseer_chain \
  --backup-root ./chainseer_backups backup

python -X utf8 chainseer_memory.py --root ./chainseer_chain \
  --backup-root ./chainseer_backups verify-backup BACKUP_PATH

python -X utf8 chainseer_memory.py --root ./chainseer_chain \
  --backup-root ./chainseer_backups restore BACKUP_PATH EMPTY_DESTINATION

python -X utf8 chainseer_memory.py --root ./chainseer_chain \
  --backup-root ./chainseer_backups drill
```

Restore is intentionally nondestructive: it refuses the live Timechain root
and every non-empty destination. A drill restores into an isolated temporary
root, verifies the chain and epoch, checks the governance and Outcome Ledgers,
rebuilds the temporal graph, compares the projection hash, runs a cited recall
probe when eligible evidence exists, records RPO/RTO, and leaves the live root
untouched. The latest drill receipt is a non-authoritative status sidecar.
The status API also reports how many Rings the latest drill snapshot trails the
current head, so a successful but older drill is never presented as current.

## Honest boundaries

- A citation proves what the verified Ring committed; it does not replay a
  historical external endpoint or assert that the cited state is still live.
- Legacy Rings remain tamper-evident but are not promoted into evidence-bound
  recall claims when the original analysis did not seal its evidence manifest.
- A successful local drill proves the snapshot can be restored on the current
  runtime. Production operators still need an off-host/off-volume schedule and
  periodic restore drills in the actual disaster-recovery environment.
- The Memory Core has no wallet, key loading, signing, broadcast, or live
  capital capability.
