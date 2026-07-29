# Chainseer entity and insider evidence graph

The entity graph turns evidence already collected by Chainseer into explicit,
machine-readable relationships. Its purpose is to answer:

- Which entities have a verified or provider-attested role around this token?
- Which addresses control supply, contract privileges, or liquidity?
- Does one entity appear in multiple privileged roles?
- Which conclusions are direct evidence, and which important links remain
  unknown?

The graph does not infer an insider merely because two wallets behave alike.
It does not invent shared funding, beneficial ownership, or off-chain identity.

## Schema

Every fresh analysis exposes `entity_graph` in the public report:

```json
{
  "schema_version": "1.0",
  "network": "robinhood",
  "root_entity_id": "entity-...",
  "anchor": {"type": "block_pin", "value": 123456},
  "summary": {
    "entity_count": 8,
    "relationship_count": 9,
    "privileged_entity_count": 2,
    "confirmed_relationship_count": 3,
    "provider_attested_relationship_count": 6,
    "signal_count": 2,
    "high_or_critical_signal_count": 1,
    "insider_risk_level": "High",
    "coverage": "measured",
    "scoring_scope": "evidence_only",
    "changes_legitimacy_score": false
  },
  "nodes": [],
  "edges": [],
  "signals": [],
  "limitations": [],
  "graph_hash": "..."
}
```

The canonical SHA-256 `graph_hash` covers every field except itself. Graph
verification also rejects a missing root entity, a dangling edge, or a signal
that references an unknown node.

## Nodes

Nodes are deduplicated deterministic entities. Common roles include:

| Network | Entity types and roles |
| --- | --- |
| Robinhood Chain | analyzed token, deployer, reported creator, contract owner, primary market, LP withdrawal controller, proxy implementation, top holder, AMM contract |
| Solana | analyzed mint, mint authority, freeze authority, primary market, largest token account, resolved token-account owner |

Node roles can merge when the exact same address appears in more than one
evidence source. A role merge records address equality; it does not prove that
one natural person beneficially owns every role.

## Relationships

Edges use one of four evidence states:

| Evidence status | Meaning |
| --- | --- |
| `onchain_confirmed` | Read directly from anchored RPC state |
| `cross_source_confirmed` | Independent on-chain/provider observations bind the same relationship |
| `provider_attested` | Reported by Blockscout, GoPlus, DexScreener, or another named provider |
| `unresolved` | A relationship is represented but its controller or identity is not proven |

Each edge includes:

- deterministic source and target node IDs
- relationship type
- evidence status and confidence
- evidence fact references
- bounded relationship attributes such as holder rank, supply percentage, or
  verified withdrawal percentage

Current relationship types include:

- `deployed`
- `controls_contract`
- `implementation_for`
- `market_for`
- `holds`
- `controls_liquidity`
- `controls_token_account`
- `can_mint`
- `can_freeze`

## Insider-exposure signals

Signals are evidence summaries, not accusations:

| Signal | Meaning |
| --- | --- |
| `privileged_role_overlap` | One exact address has two or more privileged roles |
| `privileged_supply_concentration` | A privileged entity is also a material holder |
| `direct_liquidity_control` | A verified controller can withdraw a material LP position |
| `serial_deployer` | Explorer evidence reports a deployer with many created contracts |
| `scam_flagged_entity` | A deployer or top holder is provider-flagged |
| `creator_source_disagreement` | Creator/deployer providers disagree |
| `proxy_holder` | A top holder is a proxy whose controlling party is unresolved |
| `active_mint_authority` | A Solana authority can increase supply |
| `active_freeze_authority` | A Solana authority can freeze token accounts |
| `authority_controls_large_token_account` | An active Solana authority also controls a large token account |

Severity is `info`, `caution`, `high`, or `critical`. The summary maps the
highest supported signal to `Low`, `Elevated`, `High`, or `Critical`.
`Unknown` means privileged-entity coverage is insufficient.

## Network-specific interpretation

### Robinhood Chain

The graph can join:

- Blockscout deployer and creation-transaction evidence
- GoPlus reported creator and owner
- proxy implementation metadata
- DexScreener/contract-confirmed primary markets
- Blockscout top holders against pinned total supply
- creator-controlled V2 LP positions verified by LP supply and holder evidence

Provider disagreement is preserved as separate candidate entities. Chainseer
does not silently choose one identity and discard the conflict.

### Solana

The graph can join:

- confirmed mint and freeze authorities
- largest SPL token accounts
- owners resolved through `getMultipleAccounts`
- DexScreener primary-market context

Largest token accounts may be AMM or program vaults. The graph therefore marks
their vault identity as unresolved and does not treat an ordinary large token
account as an insider. A high-confidence authority/account overlap is only
reported when the exact authority address is the resolved account owner.

## Scoring boundary

Entity graph v1 is `evidence_only`. It does not change Chainseer's legitimacy
score or weaken/override any hard stop. Existing independently verified
conditions—such as creator-withdrawable liquidity, active Solana authorities,
or extreme concentration—continue to affect deterministic analysis through
their established controls.

This separation allows the benchmark to collect graph observations before any
new graph-based scoring policy is proposed. Future score changes must be
versioned, independently tested, and benchmarked against matured outcomes.

## Timechain and API

Before sealing:

1. Chainseer builds the graph from structured analysis evidence.
2. The canonical hash and graph references are verified.
3. The cognitive intake receives only bounded summary fields and signal codes.
4. The Timechain analysis ring seals the graph hash, summary, and signals.
5. The authenticated public API returns bounded nodes, edges, signals,
   limitations, and the same graph hash.

The public report schema is `1.2`. Benchmark capture accepts both historical
schema `1.1` reports and graph-enabled `1.2` reports, so existing observations
remain valid.

## Explicit limitations

The current graph does not establish:

- shared wallet funding or common source-of-funds
- coordinated transaction timing or behavioral clusters
- beneficial ownership behind contracts, multisigs, custodians, or exchanges
- off-chain legal identity
- Solana deployer/creator attribution for a generic mint
- Solana AMM/program-vault identities without independent binding

These are reported as coverage gaps. Adding them requires direct, provenance-
tracked evidence rather than similarity-only inference.
