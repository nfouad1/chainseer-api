"""Deterministic entity and insider-evidence graphs for Chainseer reports.

The graph is an evidence projection, not a wallet-clustering oracle. It records
only relationships supported by the analysis inputs and explicitly names
coverage gaps such as funding flows and behavioral linkage.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from typing import Any, Iterable


GRAPH_SCHEMA_VERSION = "1.0"
EVM_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
EVM_TX_HASH_RE = re.compile(r"^0x[a-fA-F0-9]{64}$")
ZERO_EVM_ADDRESS = "0x" + "0" * 40
PRIVILEGED_ROLES = {
    "contract_owner",
    "deployer",
    "liquidity_controller",
    "mint_authority",
    "freeze_authority",
}
SEVERITY_RANK = {"info": 0, "caution": 1, "high": 2, "critical": 3}


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return 0


def _clean_scalar(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, (int, bool)):
        return value
    if isinstance(value, str):
        return value[:256]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return str(value)


def _normalize_identifier(network: str, identifier: Any) -> str | None:
    value = str(identifier or "").strip()
    if not value or len(value) > 128:
        return None
    if network == "robinhood":
        if not EVM_ADDRESS_RE.fullmatch(value):
            return None
        value = value.lower()
        if value == ZERO_EVM_ADDRESS:
            return None
    return value


def _refs(*groups: Iterable[Any] | None) -> list[str]:
    result: list[str] = []
    for group in groups:
        for value in group or []:
            if isinstance(value, dict):
                value = value.get("fact_id") or value.get("id")
            item = str(value or "").strip()
            if item and item not in result:
                result.append(item)
    return result


class _Graph:
    def __init__(
        self,
        network: str,
        token_address: str,
        *,
        anchor_type: str,
        anchor_value: int | str | None,
    ):
        self.network = network
        self.nodes: dict[str, dict[str, Any]] = {}
        self.node_keys: dict[tuple[str, str], str] = {}
        self.edges: dict[str, dict[str, Any]] = {}
        self.signals: dict[str, dict[str, Any]] = {}
        self.limitations: list[str] = []
        self.root_entity_id = self.add_node(
            "token",
            token_address,
            roles=["analyzed_asset"],
            label="Analyzed token",
        )
        self.anchor = {"type": anchor_type, "value": anchor_value}

    def add_node(
        self,
        entity_type: str,
        identifier: Any,
        *,
        roles: Iterable[str] = (),
        label: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> str | None:
        normalized = _normalize_identifier(self.network, identifier)
        if normalized is None:
            return None
        key = (entity_type, normalized)
        node_id = self.node_keys.get(key)
        if node_id is None:
            node_id = (
                f"entity-{canonical_hash([self.network, entity_type, normalized])[:20]}"
            )
            self.node_keys[key] = node_id
            self.nodes[node_id] = {
                "id": node_id,
                "type": entity_type,
                "address": normalized,
                "label": label or entity_type.replace("_", " ").title(),
                "roles": [],
                "attributes": {},
            }
        node = self.nodes[node_id]
        node["roles"] = sorted(
            set(node["roles"]).union(str(role) for role in roles if role)
        )
        if label and node["label"] == entity_type.replace("_", " ").title():
            node["label"] = label
        for name, value in (attributes or {}).items():
            cleaned = _clean_scalar(value)
            if cleaned is not None and cleaned != "":
                node["attributes"][str(name)] = cleaned
        return node_id

    def add_edge(
        self,
        source_id: str | None,
        target_id: str | None,
        relationship: str,
        *,
        evidence_status: str,
        confidence: str,
        evidence_refs: Iterable[Any] = (),
        attributes: dict[str, Any] | None = None,
    ) -> str | None:
        if source_id not in self.nodes or target_id not in self.nodes:
            return None
        edge_base = {
            "source": source_id,
            "target": target_id,
            "relationship": relationship,
            "evidence_status": evidence_status,
            "confidence": confidence,
            "evidence_refs": _refs(evidence_refs),
            "attributes": {
                str(name): cleaned
                for name, value in (attributes or {}).items()
                if (cleaned := _clean_scalar(value)) is not None
                and cleaned != ""
            },
        }
        edge_id = f"edge-{canonical_hash(edge_base)[:20]}"
        self.edges[edge_id] = {"id": edge_id, **edge_base}
        return edge_id

    def add_signal(
        self,
        code: str,
        severity: str,
        reason: str,
        *,
        entity_ids: Iterable[str] = (),
        evidence_refs: Iterable[Any] = (),
        confidence: str = "moderate",
    ) -> None:
        subjects = sorted(
            {item for item in entity_ids if item in self.nodes}
        )
        signal_base = {
            "code": code,
            "severity": severity,
            "reason": reason,
            "entity_ids": subjects,
            "evidence_refs": _refs(evidence_refs),
            "confidence": confidence,
        }
        signal_id = f"signal-{canonical_hash(signal_base)[:20]}"
        self.signals[signal_id] = {"id": signal_id, **signal_base}

    def finalize(self) -> dict[str, Any]:
        privileged = [
            node
            for node in self.nodes.values()
            if PRIVILEGED_ROLES.intersection(node["roles"])
        ]
        for node in privileged:
            overlap = sorted(PRIVILEGED_ROLES.intersection(node["roles"]))
            if len(overlap) >= 2:
                self.add_signal(
                    "privileged_role_overlap",
                    "caution",
                    (
                        f"One address holds multiple privileged roles: "
                        f"{', '.join(overlap)}."
                    ),
                    entity_ids=[node["id"]],
                    confidence="high",
                )

        for edge in self.edges.values():
            if edge["relationship"] != "holds":
                continue
            holder = self.nodes[edge["source"]]
            if not PRIVILEGED_ROLES.intersection(holder["roles"]):
                continue
            pct = _safe_float(edge["attributes"].get("pct_total_supply"))
            if pct is None or pct < 5:
                continue
            severity = "critical" if pct >= 50 else "high" if pct >= 20 else "caution"
            self.add_signal(
                "privileged_supply_concentration",
                severity,
                (
                    f"A privileged entity is observed holding "
                    f"{pct:.2f}% of total supply."
                ),
                entity_ids=[holder["id"], self.root_entity_id],
                evidence_refs=edge["evidence_refs"],
                confidence=edge["confidence"],
            )

        ordered_signals = sorted(
            self.signals.values(),
            key=lambda item: (
                -SEVERITY_RANK.get(item["severity"], 0),
                item["code"],
                item["id"],
            ),
        )
        if ordered_signals:
            top = ordered_signals[0]["severity"]
            insider_risk = {
                "critical": "Critical",
                "high": "High",
                "caution": "Elevated",
                "info": "Low",
            }[top]
        elif privileged:
            insider_risk = "Low"
        else:
            insider_risk = "Unknown"
        measured_relationships = sum(
            edge["evidence_status"]
            in {"onchain_confirmed", "cross_source_confirmed"}
            for edge in self.edges.values()
        )
        provider_relationships = sum(
            edge["evidence_status"] == "provider_attested"
            for edge in self.edges.values()
        )
        nodes = sorted(
            self.nodes.values(),
            key=lambda item: (item["type"], item["address"], item["id"]),
        )
        edges = sorted(
            self.edges.values(),
            key=lambda item: (
                item["relationship"],
                item["source"],
                item["target"],
                item["id"],
            ),
        )
        graph = {
            "schema_version": GRAPH_SCHEMA_VERSION,
            "network": self.network,
            "root_entity_id": self.root_entity_id,
            "anchor": self.anchor,
            "summary": {
                "entity_count": len(nodes),
                "relationship_count": len(edges),
                "privileged_entity_count": len(privileged),
                "confirmed_relationship_count": measured_relationships,
                "provider_attested_relationship_count": provider_relationships,
                "signal_count": len(ordered_signals),
                "high_or_critical_signal_count": sum(
                    item["severity"] in {"high", "critical"}
                    for item in ordered_signals
                ),
                "insider_risk_level": insider_risk,
                "coverage": (
                    "measured" if privileged or len(nodes) > 1 else "limited"
                ),
                "scoring_scope": "evidence_only",
                "changes_legitimacy_score": False,
            },
            "nodes": nodes,
            "edges": edges,
            "signals": ordered_signals,
            "limitations": sorted(set(self.limitations)),
        }
        graph["graph_hash"] = canonical_hash(graph)
        return graph


def _claim_refs(
    claim_evidence: dict[str, Any] | None, *names: str
) -> list[str]:
    claims = claim_evidence or {}
    return _refs(*(claims.get(name) for name in names))


def build_robinhood_entity_graph(
    token_address: str,
    data: dict[str, Any],
    *,
    block_pin: int | None = None,
    claim_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    graph = _Graph(
        "robinhood",
        token_address,
        anchor_type="block_pin",
        anchor_value=block_pin,
    )
    token_id = graph.root_entity_id
    goplus = data.get("goplus_security") or {}
    blockscout = data.get("blockscout_address") or {}
    deployer_data = data.get("deployer") or {}
    serial_deployer = deployer_data.get("serial_deployer") or {}
    holders = data.get("blockscout_holders") or {}
    dex = data.get("dex_pairs") or {}
    lp = data.get("lp_lock") or {}
    source = data.get("source_code") or {}
    external_refs = _claim_refs(claim_evidence, "external_apis")
    holder_refs = _claim_refs(
        claim_evidence, "activity_and_holders"
    )
    pair_refs = _claim_refs(claim_evidence, "dex_pairs")
    lp_refs = _claim_refs(claim_evidence, "lp_lock")

    creators_by_source = {
        "blockscout": blockscout.get("creator_address")
        or deployer_data.get("creator_address"),
        "goplus": goplus.get("creator_address"),
    }
    normalized_creators: dict[str, str] = {}
    creation_tx_hash = str(
        deployer_data.get("creation_tx_hash") or ""
    ).strip()
    if not EVM_TX_HASH_RE.fullmatch(creation_tx_hash):
        creation_tx_hash = ""
    for source_name, address in creators_by_source.items():
        normalized = _normalize_identifier("robinhood", address)
        if normalized:
            normalized_creators[source_name] = normalized
            roles = ["deployer"] if source_name == "blockscout" else [
                "reported_creator"
            ]
            node_id = graph.add_node(
                "address",
                normalized,
                roles=roles,
                label="Token deployer" if source_name == "blockscout" else "Reported creator",
                attributes=(
                    {
                        "serial_deployer": bool(
                            deployer_data.get("is_serial_deployer")
                        ),
                        "created_contract_count": deployer_data.get(
                            "total_deployer_creations"
                        ),
                        "scam_flagged": bool(
                            deployer_data.get("deployer_is_scam")
                            or deployer_data.get("is_scam")
                            or serial_deployer.get("deployer_is_scam")
                        ),
                    }
                    if source_name == "blockscout"
                    else None
                ),
            )
            graph.add_edge(
                node_id,
                token_id,
                "deployed",
                evidence_status="provider_attested",
                confidence=(
                    "high"
                    if source_name == "blockscout"
                    and creation_tx_hash
                    else "moderate"
                ),
                evidence_refs=(
                    holder_refs if source_name == "blockscout" else external_refs
                ),
                attributes={
                    "source": source_name,
                    "creation_tx_hash": (
                        creation_tx_hash
                        if source_name == "blockscout"
                        else None
                    ),
                },
            )
            if (
                source_name == "blockscout"
                and deployer_data.get("is_serial_deployer")
            ):
                graph.add_signal(
                    "serial_deployer",
                    "high",
                    (
                        "The deployer is reported to have created "
                        f"{_safe_int(deployer_data.get('total_deployer_creations'))} "
                        "contracts."
                    ),
                    entity_ids=[node_id],
                    evidence_refs=holder_refs,
                )
            if (
                source_name == "blockscout"
                and (
                    deployer_data.get("deployer_is_scam")
                    or deployer_data.get("is_scam")
                    or serial_deployer.get("deployer_is_scam")
                )
            ):
                graph.add_signal(
                    "scam_flagged_entity",
                    "critical",
                    "The deployer is scam-flagged by the explorer evidence.",
                    entity_ids=[node_id],
                    evidence_refs=holder_refs,
                )
    if len(set(normalized_creators.values())) > 1:
        graph.add_signal(
            "creator_source_disagreement",
            "caution",
            "Blockscout and GoPlus report different creator/deployer addresses.",
            entity_ids=[
                graph.node_keys[("address", address)]
                for address in sorted(set(normalized_creators.values()))
            ],
            evidence_refs=_refs(external_refs, holder_refs),
            confidence="high",
        )

    owner = _normalize_identifier(
        "robinhood", goplus.get("owner_address")
    )
    if owner:
        owner_id = graph.add_node(
            "address",
            owner,
            roles=["contract_owner"],
            label="Reported contract owner",
        )
        graph.add_edge(
            owner_id,
            token_id,
            "controls_contract",
            evidence_status="provider_attested",
            confidence="moderate",
            evidence_refs=external_refs,
            attributes={"source": "goplus"},
        )

    pair_address = dex.get("primary_pair_address")
    pair_id = graph.add_node(
        "market",
        pair_address,
        roles=["primary_market"],
        label="Primary market",
        attributes={
            "amm_version": dex.get("primary_amm_version"),
            "liquidity_usd": dex.get("primary_liquidity_usd"),
        },
    )
    graph.add_edge(
        pair_id,
        token_id,
        "market_for",
        evidence_status="cross_source_confirmed",
        confidence="high",
        evidence_refs=pair_refs,
    )

    controller = lp.get("withdrawal_controller")
    controller_id = graph.add_node(
        "address",
        controller,
        roles=["liquidity_controller"],
        label="Liquidity withdrawal controller",
        attributes={
            "withdrawable_pct": lp.get("withdrawable_pct"),
            "custody_state": lp.get("state"),
        },
    )
    graph.add_edge(
        controller_id,
        pair_id,
        "controls_liquidity",
        evidence_status="cross_source_confirmed",
        confidence="high" if lp.get("withdrawal_verified") else "moderate",
        evidence_refs=lp_refs,
        attributes={
            "withdrawable_pct": lp.get("withdrawable_pct"),
            "withdrawal_verified": bool(lp.get("withdrawal_verified")),
        },
    )
    withdrawable = _safe_float(lp.get("withdrawable_pct"))
    if controller_id and lp.get("withdrawal_verified") and withdrawable is not None:
        graph.add_signal(
            "direct_liquidity_control",
            "critical" if withdrawable >= 50 else "high",
            (
                f"One entity can directly withdraw {withdrawable:.2f}% "
                "of the primary liquidity position."
            ),
            entity_ids=[controller_id, pair_id],
            evidence_refs=lp_refs,
            confidence="high",
        )

    implementations = source.get("implementations") or []
    for item in implementations[:10]:
        if not isinstance(item, dict):
            continue
        implementation = (
            item.get("address_hash")
            or item.get("address")
            or item.get("implementation_address")
        )
        implementation_id = graph.add_node(
            "contract",
            implementation,
            roles=["proxy_implementation"],
            label="Proxy implementation",
            attributes={"verified": item.get("is_verified")},
        )
        graph.add_edge(
            implementation_id,
            token_id,
            "implementation_for",
            evidence_status="provider_attested",
            confidence="high" if item.get("is_verified") else "moderate",
            evidence_refs=_refs(
                external_refs,
                [source.get("fact_id")],
            ),
        )

    denominator = _safe_int(
        holders.get("concentration_denominator_raw")
        or holders.get("total_supply_raw")
    )
    verified_amm = {
        str(value).lower()
        for value in holders.get("verified_amm_addresses") or []
    }
    proxy_holders = {
        str(item.get("address") or "").lower(): item.get("proxy_type")
        for item in holders.get("proxy_holders") or []
        if isinstance(item, dict)
    }
    scam_holders = {
        str(value).lower()
        for value in holders.get("scam_flagged_holders") or []
    }
    for rank, item in enumerate((holders.get("holders") or [])[:20], 1):
        if not isinstance(item, dict):
            continue
        address = _normalize_identifier("robinhood", item.get("address"))
        if not address:
            continue
        is_market = address in verified_amm
        node_type = "market" if is_market else (
            "contract" if item.get("is_contract") else "address"
        )
        holder_id = graph.add_node(
            node_type,
            address,
            roles=(
                ["primary_market", "amm_contract"]
                if is_market
                else ["top_holder"]
            ),
            label=(
                "Verified AMM market"
                if is_market
                else f"Top holder #{rank}"
            ),
            attributes={
                "holder_rank": rank,
                "is_contract": bool(item.get("is_contract")),
                "proxy_type": proxy_holders.get(address),
                "scam_flagged": address in scam_holders,
            },
        )
        amount = _safe_int(
            item.get("balance_parsed") or item.get("balance_raw")
        )
        pct = round(amount / denominator * 100, 4) if denominator > 0 else None
        graph.add_edge(
            holder_id,
            token_id,
            "holds",
            evidence_status="provider_attested",
            confidence="high" if denominator > 0 else "moderate",
            evidence_refs=holder_refs,
            attributes={
                "rank": rank,
                "amount_raw": amount,
                "pct_total_supply": pct,
                "excluded_from_concentration": is_market,
            },
        )
        if address in scam_holders:
            graph.add_signal(
                "scam_flagged_entity",
                "critical",
                "A top holder is scam-flagged by the explorer evidence.",
                entity_ids=[holder_id],
                evidence_refs=holder_refs,
            )
        if address in proxy_holders:
            graph.add_signal(
                "proxy_holder",
                "caution",
                (
                    "A top holder is an upgradeable or minimal proxy contract; "
                    "its controlling party is not resolved."
                ),
                entity_ids=[holder_id],
                evidence_refs=holder_refs,
            )

    graph.limitations.extend(
        [
            (
                "Shared funding sources, transaction-behavior clusters, and "
                "off-chain identity are not inferred without direct evidence."
            ),
            (
                "Blockscout, GoPlus, and DexScreener relationships are "
                "provider-attested unless explicitly cross-source confirmed."
            ),
            (
                "A role overlap is an exposure indicator, not proof of "
                "coordination, fraud, or beneficial ownership."
            ),
        ]
    )
    return graph.finalize()


def build_solana_entity_graph(
    mint: str,
    data: dict[str, Any],
    *,
    slot_anchor: int | None = None,
    facts: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    graph = _Graph(
        "solana",
        mint,
        anchor_type="confirmed_slot_anchor",
        anchor_value=slot_anchor,
    )
    token_id = graph.root_entity_id
    basic = data.get("basic_info") or {}
    holders = data.get("holder_concentration") or {}
    dex = data.get("dex_pairs") or {}
    fact_ids = [
        str(item.get("fact_id"))
        for item in facts
        if isinstance(item, dict) and item.get("fact_id")
    ]
    mint_refs = [value for value in fact_ids if value in {"solana-01", "solana-02"}]
    holder_refs = [value for value in fact_ids if value in {"solana-03", "solana-04"}]

    for role, relationship, address in (
        ("mint_authority", "can_mint", basic.get("mint_authority")),
        ("freeze_authority", "can_freeze", basic.get("freeze_authority")),
    ):
        authority_id = graph.add_node(
            "address",
            address,
            roles=[role],
            label=role.replace("_", " ").title(),
        )
        graph.add_edge(
            authority_id,
            token_id,
            relationship,
            evidence_status="onchain_confirmed",
            confidence="high",
            evidence_refs=mint_refs,
        )
        if authority_id:
            graph.add_signal(
                f"active_{role}",
                "high",
                (
                    "The active mint authority can increase supply."
                    if role == "mint_authority"
                    else "The active freeze authority can restrict token accounts."
                ),
                entity_ids=[authority_id, token_id],
                evidence_refs=mint_refs,
                confidence="high",
            )

    pair_id = graph.add_node(
        "market",
        dex.get("primary_pair"),
        roles=["primary_market"],
        label="Primary Solana market",
        attributes={
            "dex": dex.get("primary_amm_version"),
            "liquidity_usd": dex.get("total_liquidity_usd"),
        },
    )
    graph.add_edge(
        pair_id,
        token_id,
        "market_for",
        evidence_status="provider_attested",
        confidence="moderate",
        evidence_refs=[
            value
            for value in fact_ids
            if value not in {"solana-01", "solana-02", "solana-03", "solana-04"}
        ],
        attributes={"source": "dexscreener"},
    )

    for rank, item in enumerate((holders.get("largest_accounts") or [])[:20], 1):
        if not isinstance(item, dict):
            continue
        token_account_id = graph.add_node(
            "token_account",
            item.get("token_account"),
            roles=["top_token_account"],
            label=f"Largest token account #{rank}",
            attributes={"holder_rank": rank},
        )
        owner_id = graph.add_node(
            "address",
            item.get("owner"),
            roles=["token_account_owner"],
            label=f"Resolved owner #{rank}",
        )
        graph.add_edge(
            owner_id,
            token_account_id,
            "controls_token_account",
            evidence_status="onchain_confirmed",
            confidence="high",
            evidence_refs=holder_refs,
        )
        graph.add_edge(
            token_account_id,
            token_id,
            "holds",
            evidence_status="onchain_confirmed",
            confidence="high",
            evidence_refs=holder_refs,
            attributes={
                "rank": rank,
                "amount_raw": item.get("amount_raw"),
                "pct_total_supply": item.get("pct_total_supply"),
                "vault_identity_resolved": False,
            },
        )
        if owner_id and token_account_id:
            owner_node = graph.nodes[owner_id]
            owner_roles = set(owner_node["roles"])
            privileged = PRIVILEGED_ROLES.intersection(owner_roles)
            pct = _safe_float(item.get("pct_total_supply"))
            if privileged and pct is not None and pct >= 5:
                graph.add_signal(
                    "authority_controls_large_token_account",
                    "critical" if pct >= 50 else "high" if pct >= 20 else "caution",
                    (
                        f"An active authority controls a token account holding "
                        f"{pct:.2f}% of supply."
                    ),
                    entity_ids=[owner_id, token_account_id, token_id],
                    evidence_refs=_refs(mint_refs, holder_refs),
                    confidence="high",
                )

    graph.limitations.extend(
        [
            (
                "Largest SPL token accounts can be AMM or program vaults; "
                "vault identities are not excluded until independently bound."
            ),
            (
                "Generic SPL analysis does not infer deployer, creator, shared "
                "funding, or behavioral wallet clusters."
            ),
            (
                "An authority or account-owner overlap is an exposure "
                "indicator, not proof of malicious coordination."
            ),
        ]
    )
    return graph.finalize()


def verify_entity_graph(graph: dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(graph, dict):
        return False, "graph_not_object"
    expected = graph.get("graph_hash")
    if not isinstance(expected, str) or len(expected) != 64:
        return False, "graph_hash_missing"
    unsigned = {key: value for key, value in graph.items() if key != "graph_hash"}
    if canonical_hash(unsigned) != expected:
        return False, "graph_hash_mismatch"
    node_ids = {
        item.get("id")
        for item in graph.get("nodes") or []
        if isinstance(item, dict)
    }
    if graph.get("root_entity_id") not in node_ids:
        return False, "root_entity_missing"
    for edge in graph.get("edges") or []:
        if not isinstance(edge, dict):
            return False, "edge_not_object"
        if edge.get("source") not in node_ids or edge.get("target") not in node_ids:
            return False, "dangling_edge"
    for signal in graph.get("signals") or []:
        if not isinstance(signal, dict):
            return False, "signal_not_object"
        if any(item not in node_ids for item in signal.get("entity_ids") or []):
            return False, "dangling_signal"
    return True, "verified"
