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

## Schema versions

Every Memory Core surface stamps the schema version of the payload it
returns, so a consumer can detect a format change instead of silently
misreading a field. All are independently versioned:

| Constant | Module | Version |
| --- | --- | --- |
| `MEMORY_SCHEMA_VERSION` | `chainseer_memory.py` | `1.0` |
| `BACKUP_SCHEMA_VERSION` | `chainseer_memory.py` | `1.0` |
| `OUTCOME_LEDGER_SCHEMA_VERSION` | `chainseer_outcome_ledger.py` | `1.0` |
| `EVIDENCE_MANIFEST_SCHEMA_VERSION` | `chainseer_outcome_ledger.py` | `1.0` |
| `TEMPORAL_GRAPH_SCHEMA_VERSION` | `chainseer_temporal_graph.py` | `1.0` |
| `GOVERNANCE_SCHEMA_VERSION` | `chainseer_governance.py` | `1.0` |

Recall responses, citation proofs, status payloads, backup manifests, and
the persisted temporal projection all carry `schema_version`. A projection
or backup written under an older version is rebuilt rather than migrated in
place — the Timechain is authoritative, so derived views are always
reproducible from it.

## Coverage

The Memory Core reads analysis rings by type, not by chain root. Only
these types are recognised (`SUPPORTED_ANALYSIS_RING_TYPES`):

- `token_analysis` — Robinhood Chain (`chainseer.py`)
- `base_launch_analysis` — Base (`chainseer_base.py`)
- `solana_token_analysis` — the public on-demand Solana analyser
  (`chainseer_solana_public.py`)

Two production ring types are **not** covered today, and their histories
are therefore absent from the Outcome Ledger, the temporal Entity Graph,
and recall:

- `solana_launch_analysis` — the Solana autotrader (`chainseer_solana.py`),
  sealed into its own `solana_chain` root
- `pons_launch_analysis` — Pons (`chainseer_pons.py`), sealed into its own
  `pons_chain` root

These are separate authoritative Timechains with separate faculty
registries by design. Nothing federates them into a shared Outcome Ledger
or Entity Graph yet: there is no cross-chain evidence-signing or citation
scheme, so a claim cannot cite a ring from a chain other than the one it
was recalled from. Treat cross-system memory as unbuilt, not as implied by
the five pillars.

## Expected scale

Measured on a live chain, not extrapolated:

| Chain | Rings | Analysis rings | Subjects | Projection rebuild |
| --- | --- | --- | --- | --- |
| `chainseer_chain` | 1,395 | 189 | 168 | 0.06–0.16 s |

Rebuild cost tracks the number of *recognised analysis rings*, not total
ring count: rebuilding over a 3,223-ring chain whose rings are all of an
unrecognised type completes in ~0.02 s because nothing matches. A full
rebuild at this size is cheap enough to run on demand, which is why
`TemporalGraphStore` can fall back to a full rebuild whenever its
incremental cursor fails validation.

This has not been load-tested beyond the sizes above. Operators planning
for a substantially larger corpus should re-measure before assuming the
on-demand rebuild stays inexpensive, and prefer `refresh()` (incremental)
over `rebuild()` on the hot path regardless.

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
