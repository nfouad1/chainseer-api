"""Production, on-demand Solana mint analysis for Chainseer.

This adapter deliberately does not expose the paper/shadow learner as a public
scanner.  The learner is Pump.fun launch-catalog specific; the public adapter
accepts any valid SPL mint and only claims launch provenance when it is actually
available.  Deterministic chain and market evidence remains authoritative, while
the shared Chainseer CognitiveLoop may lower confidence or grow faculties but
may never remove a hard stop or raise a deterministic component score.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from chainseer_entity_graph import (
    build_solana_entity_graph,
    verify_entity_graph,
)
from chainseer_pumpfun_provenance import (
    PUMP_AMM_PROGRAM_ID,
    creator_deployment_history,
    resolve_genesis_creator,
)


BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_INDEX = {char: index for index, char in enumerate(BASE58_ALPHABET)}
SOLSCAN_TOKEN_URL = "https://solscan.io/token/"
JUPITER_API_URL = "https://api.jup.ag"
DEXSCREENER_API_URL = "https://api.dexscreener.com"
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"
RISKY_TOKEN_2022_EXTENSIONS = {
    "confidentialtransfermint",
    "defaultaccountstate",
    "permanentdelegate",
    "pausableconfig",
    "nontransferable",
    "transferfeeconfig",
    "transferhook",
}
REQUEST_EXCEPTION = getattr(
    getattr(requests, "exceptions", None),
    "RequestException",
    OSError,
)


class InfrastructureIndeterminateError(RuntimeError):
    """An external observation could not be completed reliably."""


class SolanaRPC:
    """Small confirmed-state RPC client for the public scanner."""

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 15.0,
        session: requests.Session | None = None,
        max_retries: int = 2,
    ):
        self.url = url
        self.timeout = timeout
        self.session = session or requests.Session()
        self.max_retries = max(0, int(max_retries))
        self._request_id = 0

    def _call(self, method: str, params: list | None = None):
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self._request_id += 1
            try:
                response = self.session.post(
                    self.url,
                    json={
                        "jsonrpc": "2.0",
                        "id": self._request_id,
                        "method": method,
                        "params": params or [],
                    },
                    timeout=self.timeout,
                )
                response.raise_for_status()
                payload = response.json()
                if payload.get("error"):
                    raise ValueError(
                        f"RPC error {payload['error'].get('code')}"
                    )
                return payload.get("result")
            except (REQUEST_EXCEPTION, OSError, ValueError) as exc:
                last_error = exc
                status = getattr(
                    getattr(exc, "response", None),
                    "status_code",
                    None,
                )
                if (
                    attempt >= self.max_retries
                    or status not in {None, 408, 425, 429, 500, 502, 503, 504}
                ):
                    break
                time.sleep(min(2.0, 0.25 * (2**attempt)))
        raise InfrastructureIndeterminateError(
            f"Solana RPC {method} unavailable: {last_error}"
        ) from last_error

    def get_slot(self) -> int:
        return _safe_int(
            self._call("getSlot", [{"commitment": "confirmed"}]),
            0,
        )

    def get_account_info(self, address: str, *, encoding: str = "jsonParsed"):
        return self._call(
            "getAccountInfo",
            [
                address,
                {"encoding": encoding, "commitment": "confirmed"},
            ],
        )

    def get_token_supply(self, mint: str):
        return self._call(
            "getTokenSupply",
            [mint, {"commitment": "confirmed"}],
        )

    def get_token_largest_accounts(self, mint: str):
        return self._call(
            "getTokenLargestAccounts",
            [mint, {"commitment": "confirmed"}],
        )

    def get_signatures_for_address(
        self,
        address: str,
        *,
        limit: int = 25,
        before: str | None = None,
    ):
        options = {
            "commitment": "confirmed",
            "limit": max(1, min(1_000, int(limit))),
        }
        if before:
            options["before"] = before
        return self._call("getSignaturesForAddress", [address, options])

    def get_transaction(self, signature: str):
        return self._call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
        )

    def get_multiple_accounts(
        self,
        addresses: list[str],
        *,
        encoding: str = "jsonParsed",
    ):
        if not addresses:
            return {"value": []}
        return self._call(
            "getMultipleAccounts",
            [
                addresses,
                {"encoding": encoding, "commitment": "confirmed"},
            ],
        )

    def get_token_accounts_by_owner(self, owner: str, mint: str):
        return self._call(
            "getTokenAccountsByOwner",
            [
                owner,
                {"mint": mint},
                {"encoding": "jsonParsed", "commitment": "confirmed"},
            ],
        )


class JupiterClient:
    """Bounded keyless-or-authenticated Jupiter evidence client."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        timeout: float = 15.0,
        session: requests.Session | None = None,
    ):
        self.api_key = api_key or os.environ.get("JUPITER_API_KEY")
        self.timeout = timeout
        self.session = session or requests.Session()

    def _get(self, path: str, params: dict) -> Any:
        headers = {"x-api-key": self.api_key} if self.api_key else {}
        try:
            response = self.session.get(
                JUPITER_API_URL + path,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except (REQUEST_EXCEPTION, OSError, ValueError) as exc:
            raise InfrastructureIndeterminateError(
                f"Jupiter {path} unavailable: {exc}"
            ) from exc

    def _quote(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
    ) -> dict:
        payload = self._get(
            "/swap/v2/order",
            {
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": str(int(amount)),
            },
        )
        out_amount = _safe_int(payload.get("outAmount"), -1)
        if out_amount <= 0 or payload.get("errorCode") is not None:
            raise InfrastructureIndeterminateError(
                "Jupiter returned no executable route."
            )
        return {
            "in_amount": _safe_int(payload.get("inAmount"), amount),
            "out_amount": out_amount,
            "router": payload.get("router"),
            "price_impact_pct": _safe_float(
                payload.get("priceImpactPct"),
                None,
            ),
        }

    def roundtrip(self, mint: str, input_lamports: int) -> dict:
        buy = self._quote(WRAPPED_SOL_MINT, mint, input_lamports)
        sell = self._quote(mint, WRAPPED_SOL_MINT, buy["out_amount"])
        return {
            "buy": buy,
            "sell": sell,
            "roundtrip_retention_pct": (
                100.0 * sell["out_amount"] / input_lamports
            ),
        }

    def token_info(self, mint: str) -> dict | None:
        values = self._get("/tokens/v2/search", {"query": mint})
        if not isinstance(values, list):
            return None
        return next(
            (item for item in values if item.get("id") == mint),
            None,
        )


class DexScreenerClient:
    """Credential-free Solana market observations with a bounded TTL."""

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        session: requests.Session | None = None,
        ttl_seconds: float = 60.0,
    ):
        self.timeout = timeout
        self.session = session or requests.Session()
        self.ttl_seconds = max(0.0, float(ttl_seconds))
        self._cache: dict[str, tuple[float, list[dict]]] = {}

    def token_pairs(self, mint: str) -> list[dict]:
        cached = self._cache.get(mint)
        if cached and time.monotonic() - cached[0] <= self.ttl_seconds:
            return json.loads(_canonical_json(cached[1]))
        try:
            response = self.session.get(
                f"{DEXSCREENER_API_URL}/token-pairs/v1/solana/{mint}",
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except (REQUEST_EXCEPTION, OSError, ValueError) as exc:
            raise InfrastructureIndeterminateError(
                f"DexScreener unavailable: {exc}"
            ) from exc
        if not isinstance(payload, list):
            raise InfrastructureIndeterminateError(
                "DexScreener returned an unexpected response."
            )
        self._cache[mint] = (time.monotonic(), payload)
        return json.loads(_canonical_json(payload))


class SolanaMintError(ValueError):
    """A public, user-correctable Solana mint error."""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return result if math.isfinite(result) else default


def decode_solana_pubkey(value: str) -> bytes:
    """Decode and validate a canonical 32-byte base58 Solana public key."""
    text = value.strip()
    if not 32 <= len(text) <= 44:
        raise SolanaMintError(
            "invalid_solana_mint",
            "Enter a valid Solana mint address.",
        )
    number = 0
    try:
        for char in text:
            number = number * 58 + BASE58_INDEX[char]
    except KeyError as exc:
        raise SolanaMintError(
            "invalid_solana_mint",
            "Enter a valid Solana mint address.",
        ) from exc
    raw = (
        number.to_bytes((number.bit_length() + 7) // 8, "big")
        if number
        else b""
    )
    leading_zeroes = len(text) - len(text.lstrip("1"))
    decoded = b"\x00" * leading_zeroes + raw
    if len(decoded) != 32:
        raise SolanaMintError(
            "invalid_solana_mint",
            "Enter a valid Solana mint address.",
        )
    return decoded


def validate_solana_mint(value: str) -> str:
    normalized = value.strip()
    decode_solana_pubkey(normalized)
    return normalized


@dataclass(frozen=True)
class SolanaPublicPolicy:
    quote_amount_sol: float = 0.01
    minimum_liquidity_usd: float = 5_000.0
    caution_liquidity_usd: float = 25_000.0
    minimum_roundtrip_retention_pct: float = 72.0
    maximum_buy_price_impact_pct: float = 12.0
    new_market_seconds: int = 60 * 60


class SolanaPublicAnalyzer:
    """Analyze arbitrary SPL mints without assuming Pump.fun provenance."""

    def __init__(
        self,
        rpc_url: str,
        *,
        timechain_agent: Any | None = None,
        jupiter_api_key: str | None = None,
        rpc: SolanaRPC | None = None,
        jupiter: JupiterClient | None = None,
        dexscreener: DexScreenerClient | None = None,
        policy: SolanaPublicPolicy | None = None,
    ):
        self.rpc = rpc or SolanaRPC(rpc_url)
        self.jupiter = jupiter or JupiterClient(jupiter_api_key)
        self.dexscreener = dexscreener or DexScreenerClient(ttl_seconds=60)
        self.timechain_agent = timechain_agent
        self.policy = policy or SolanaPublicPolicy()

    @staticmethod
    def _extension_names(info: dict) -> list[str]:
        names: list[str] = []
        for value in info.get("extensions") or []:
            if isinstance(value, str):
                names.append(value)
            elif isinstance(value, dict):
                name = value.get("extension") or value.get("type")
                if name:
                    names.append(str(name))
        return sorted(set(names))

    @staticmethod
    def _market_pair(mint: str, pairs: list[dict]) -> dict | None:
        matches = []
        for pair in pairs:
            if str(pair.get("chainId") or "").lower() != "solana":
                continue
            base = pair.get("baseToken") or {}
            quote = pair.get("quoteToken") or {}
            if mint not in {base.get("address"), quote.get("address")}:
                continue
            matches.append(pair)
        if not matches:
            return None
        return max(
            matches,
            key=lambda row: _safe_float(
                (row.get("liquidity") or {}).get("usd"), 0.0
            )
            or 0.0,
        )

    @staticmethod
    def _age_label(age_seconds: int | None) -> str:
        if age_seconds is None:
            return "Age unknown"
        if age_seconds < 60 * 60:
            return "NEW (<1h)"
        if age_seconds < 24 * 60 * 60:
            return "NEW (<24h)"
        if age_seconds < 7 * 24 * 60 * 60:
            return f"{max(1, age_seconds // 86400)}d old"
        return f"{max(1, age_seconds // 604800)}w old"

    @staticmethod
    def _score_liquidity(liquidity: float | None) -> float:
        if liquidity is None:
            return 35.0
        if liquidity >= 250_000:
            return 95.0
        if liquidity >= 100_000:
            return 88.0
        if liquidity >= 25_000:
            return 72.0
        if liquidity >= 5_000:
            return 52.0
        if liquidity > 0:
            return 20.0
        return 5.0

    @staticmethod
    def _score_maturity(age_seconds: int | None) -> float:
        if age_seconds is None:
            return 35.0
        if age_seconds >= 90 * 86400:
            return 95.0
        if age_seconds >= 30 * 86400:
            return 85.0
        if age_seconds >= 7 * 86400:
            return 72.0
        if age_seconds >= 86400:
            return 55.0
        if age_seconds >= 3600:
            return 35.0
        return 15.0

    @staticmethod
    def _hard_stop(code: str, reason: str) -> dict:
        return {
            "code": code,
            "severity": "High",
            "reason": reason,
            "action": "AVOID",
        }

    @staticmethod
    def _fact(
        index: int,
        source: str,
        query: dict,
        response: Any,
        slot: int | None,
    ) -> dict:
        return {
            "fact_id": f"solana-{index:02d}",
            "source": source,
            "query_hash": _hash(query),
            "response_hash": _hash(response),
            "block": slot,
            "fetched_at": _utc_now(),
            "cache_hit": False,
        }

    def _holder_concentration(
        self, mint: str, supply_raw: int
    ) -> tuple[dict, list[dict]]:
        largest_result = self.rpc.get_token_largest_accounts(mint) or {}
        rows = list(largest_result.get("value") or [])[:20]
        addresses = [
            row.get("address") for row in rows if row.get("address")
        ]
        owners_result = self.rpc.get_multiple_accounts(
            addresses, encoding="jsonParsed"
        )
        owner_values = list((owners_result or {}).get("value") or [])
        owners: dict[str, str | None] = {}
        for address, account in zip(addresses, owner_values):
            parsed = (
                (((account or {}).get("data") or {}).get("parsed") or {})
                .get("info")
                or {}
            )
            owners[address] = parsed.get("owner")
        holders = []
        for row in rows:
            amount = _safe_int(row.get("amount"))
            holders.append(
                {
                    "token_account": row.get("address"),
                    "owner": owners.get(row.get("address")),
                    "amount_raw": amount,
                    "pct_total_supply": (
                        round(100.0 * amount / supply_raw, 4)
                        if supply_raw > 0
                        else None
                    ),
                }
            )
        top1_raw = sum(item["amount_raw"] for item in holders[:1])
        top10_raw = sum(item["amount_raw"] for item in holders[:10])
        concentration = {
            "supply_raw": supply_raw,
            "top1_total_supply_pct": (
                round(100.0 * top1_raw / supply_raw, 4)
                if supply_raw > 0
                else None
            ),
            "top10_total_supply_pct": (
                round(100.0 * top10_raw / supply_raw, 4)
                if supply_raw > 0
                else None
            ),
            "largest_accounts": holders,
            "method": "getTokenLargestAccounts_plus_owner_resolution",
            "pool_and_program_vaults_excluded": False,
            "caveat": (
                "Largest-account concentration may include AMM or program "
                "vaults; it is cautionary evidence and cannot trigger a "
                "hard stop until custody identities are independently bound."
            ),
        }
        facts = [
            self._fact(
                3,
                "solana_rpc",
                {"method": "getTokenLargestAccounts", "mint": mint},
                {
                    "count": len(rows),
                    "amounts": [item["amount_raw"] for item in holders],
                },
                (largest_result.get("context") or {}).get("slot"),
            ),
            self._fact(
                4,
                "solana_rpc",
                {"method": "getMultipleAccounts", "count": len(addresses)},
                {
                    "resolved_owner_count": sum(bool(value) for value in owners.values()),
                    "account_count": len(addresses),
                },
                (owners_result.get("context") or {}).get("slot"),
            ),
        ]
        return concentration, facts

    def _deployer_and_creator_risk(
        self, mint: str, *, supply_raw: int
    ) -> tuple[dict, dict, list[dict]]:
        """Resolve Pump.fun launch provenance (if any) and score both the
        deployer's cadence -- the Solana analog of chainseer.py's
        _analyze_deployer_and_creation serial-deployer check -- and the
        creator's current supply concentration, the analog of chainseer.py's
        GoPlus-sourced creator_percent check.

        Both stay honestly unresolved when the mint isn't a verifiable
        Pump.fun launch, matching this module's existing principle of never
        claiming provenance that wasn't actually established -- most SPL
        mints on this public, arbitrary-address endpoint won't be Pump.fun
        launches at all.
        """
        facts: list[dict] = []
        unresolved_reason = "No verified Pump.fun launch provenance for this mint."
        deployer_data: dict = {"resolved": False, "reason": unresolved_reason}
        creator_data: dict = {"resolved": False, "reason": unresolved_reason}

        try:
            event = resolve_genesis_creator(
                self.rpc.get_signatures_for_address,
                self.rpc.get_transaction,
                mint,
            )
        except InfrastructureIndeterminateError:
            event = None
        if event is None:
            return deployer_data, creator_data, facts

        history = creator_deployment_history(
            self.rpc.get_signatures_for_address,
            self.rpc.get_transaction,
            event.creator,
            exclude_mint=mint,
        )
        deployer_data = {
            "resolved": True,
            "creator": event.creator,
            "genesis_signature": event.signature,
            **history,
        }
        facts.append(
            self._fact(
                8,
                "solana_rpc",
                {
                    "method": "getSignaturesForAddress+getTransaction",
                    "purpose": "pump_fun_genesis_and_creator_cadence",
                    "mint": mint,
                    "creator": event.creator,
                },
                {
                    "prior_deployments_in_window": history.get(
                        "prior_deployments_in_window"
                    ),
                    "scanned": history.get("scanned"),
                    "scan_degraded": history.get("scan_degraded"),
                },
                event.slot,
            )
        )

        try:
            accounts = (
                self.rpc.get_token_accounts_by_owner(event.creator, mint) or {}
            )
            holding_raw = sum(
                _safe_int(
                    (
                        (
                            ((row.get("account") or {}).get("data") or {}).get(
                                "parsed"
                            )
                            or {}
                        ).get("info")
                        or {}
                    )
                    .get("tokenAmount", {})
                    .get("amount")
                )
                for row in (accounts.get("value") or [])
            )
            creator_data = {
                "resolved": True,
                "creator": event.creator,
                "holding_raw": holding_raw,
                "pct_total_supply": (
                    round(100.0 * holding_raw / supply_raw, 4)
                    if supply_raw > 0
                    else None
                ),
            }
        except InfrastructureIndeterminateError:
            creator_data = {
                "resolved": True,
                "creator": event.creator,
                "holding_raw": None,
                "pct_total_supply": None,
                "reason": "Creator's current token balance could not be verified.",
            }
        return deployer_data, creator_data, facts

    def _seal_report(self, report: dict) -> None:
        agent = self.timechain_agent
        if agent is None:
            return
        cognition = agent.cognitive_loop.prepare(report)
        analysis = report["analysis"]
        poq = report["poq_scores"]
        entity_graph = (report.get("data") or {}).get("entity_graph") or {}
        sealed_entity_graph = {
            "graph_hash": entity_graph.get("graph_hash"),
            "summary": entity_graph.get("summary") or {},
            "signals": entity_graph.get("signals") or [],
        }
        safe_context = {
            "network": "solana",
            "mint": report["token_address"],
            "slot_anchor": (report.get("provenance") or {}).get("block_pin"),
            "analysis": {
                "risk_level": analysis.get("risk_level"),
                "legitimacy_score": analysis.get("legitimacy_score"),
                "action_label": analysis.get("action_label"),
                "component_scores": analysis.get("component_scores"),
                "hard_stop_codes": [
                    item.get("code")
                    for item in analysis.get("hard_stop_overrides") or []
                ],
                "uncertain_components": sorted(
                    (analysis.get("uncertain_components") or {}).keys()
                ),
            },
            "coverage": report.get("coverage") or {},
            "entity_graph": sealed_entity_graph,
        }
        candidate = (
            f"Solana mint {report['token_address']} assessed as "
            f"{analysis['risk_level']} risk with legitimacy score "
            f"{analysis['legitimacy_score']}/100; deterministic hard stops "
            f"{'triggered' if analysis.get('hard_stop_overrides') else 'clear'}."
        )
        verdict, ring = agent.poq_module.gate_and_seal(
            agent.tc,
            candidate,
            context=_canonical_json(safe_context),
            ring_type="solana_token_analysis",
            external_scores=poq,
            frame="assertion",
            evidence_texts=[
                _canonical_json(report.get("provenance") or {}),
                _canonical_json(analysis.get("component_scores") or {}),
                _canonical_json(report.get("coverage") or {}),
                _canonical_json(sealed_entity_graph),
            ],
            extra_payload={
                "network": "solana",
                "mint": report["token_address"],
                "slot_anchor": (report.get("provenance") or {}).get("block_pin"),
                "analysis": safe_context["analysis"],
                "coverage": report.get("coverage") or {},
                "entity_graph": sealed_entity_graph,
                "cognition": cognition,
                "live_execution_enabled": False,
            },
        )
        if ring is None:
            raise RuntimeError(
                "PoQ refused Solana analysis: "
                + "; ".join(verdict.get("reasons") or [])
            )
        report["analysis_ring"] = ring["index"]
        report["analysis_ring_hash"] = ring["ring_hash"]
        report["poq_verdict"] = verdict
        agent.cognitive_loop.finalize(report, ring)

    def analyze_token(self, mint: str) -> dict:
        mint = validate_solana_mint(mint)
        facts: list[dict] = []
        infrastructure_errors: list[str] = []
        warnings: list[str] = []
        hard_stops: list[dict] = []
        unknown: dict[str, str] = {
            "creator_risk": "Creator attribution is not verified for a generic SPL mint.",
            "deployer": "Deployer history requires independently verified launch provenance.",
            "lp_lock": "Pool-vault and LP withdrawal custody are not yet verified.",
            "wash_trading": "Transaction-flow clustering is not yet available.",
        }
        green: list[str] = []

        slot_anchor = self.rpc.get_slot()
        account_result = self.rpc.get_account_info(mint, encoding="jsonParsed") or {}
        account_slot = _safe_int(
            (account_result.get("context") or {}).get("slot"), slot_anchor
        )
        value = account_result.get("value")
        if not value:
            raise SolanaMintError(
                "solana_mint_not_found",
                "No confirmed SPL mint account was found at that address.",
            )
        parsed = ((value.get("data") or {}).get("parsed") or {})
        if parsed.get("type") != "mint":
            raise SolanaMintError(
                "not_solana_mint",
                "The address exists but is not an SPL mint account.",
            )
        info = parsed.get("info") or {}
        owner_program = value.get("owner")
        if owner_program not in {TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID}:
            raise SolanaMintError(
                "unsupported_solana_token_program",
                "The account is not owned by a supported SPL Token program.",
            )
        facts.append(
            self._fact(
                1,
                "solana_rpc",
                {"method": "getAccountInfo", "mint": mint, "encoding": "jsonParsed"},
                {
                    "owner_program": owner_program,
                    "type": parsed.get("type"),
                    "decimals": info.get("decimals"),
                    "mint_authority": bool(info.get("mintAuthority")),
                    "freeze_authority": bool(info.get("freezeAuthority")),
                    "extensions": self._extension_names(info),
                },
                account_slot,
            )
        )

        supply_result = self.rpc.get_token_supply(mint) or {}
        supply_value = supply_result.get("value") or {}
        supply_raw = _safe_int(supply_value.get("amount"), _safe_int(info.get("supply")))
        decimals = _safe_int(supply_value.get("decimals"), _safe_int(info.get("decimals")))
        facts.append(
            self._fact(
                2,
                "solana_rpc",
                {"method": "getTokenSupply", "mint": mint},
                {"amount": supply_raw, "decimals": decimals},
                _safe_int((supply_result.get("context") or {}).get("slot"), account_slot),
            )
        )

        mint_authority = info.get("mintAuthority")
        freeze_authority = info.get("freezeAuthority")
        extensions = self._extension_names(info)
        risky_extensions = sorted(
            name
            for name in extensions
            if name.replace("_", "").lower() in RISKY_TOKEN_2022_EXTENSIONS
        )
        if mint_authority:
            hard_stops.append(
                self._hard_stop(
                    "mint_authority_active",
                    "The mint authority can still increase token supply.",
                )
            )
        else:
            green.append("Mint authority is revoked.")
        if freeze_authority:
            hard_stops.append(
                self._hard_stop(
                    "freeze_authority_active",
                    "The freeze authority can restrict token accounts.",
                )
            )
        else:
            green.append("Freeze authority is revoked.")
        if risky_extensions:
            hard_stops.append(
                self._hard_stop(
                    "risky_token_2022_extensions",
                    "Risk-sensitive Token-2022 extensions are active: "
                    + ", ".join(risky_extensions),
                )
            )
        if supply_raw <= 0:
            hard_stops.append(
                self._hard_stop(
                    "zero_or_unresolved_supply",
                    "Confirmed token supply is zero or unresolved.",
                )
            )

        concentration: dict = {}
        try:
            concentration, concentration_facts = self._holder_concentration(
                mint, supply_raw
            )
            facts.extend(concentration_facts)
            top1 = _safe_float(
                concentration.get("top1_total_supply_pct"), None
            )
            top10 = _safe_float(
                concentration.get("top10_total_supply_pct"), None
            )
            if top1 is not None and top1 > 50:
                warnings.append("Largest token account exceeds 50% of supply.")
            if top10 is not None and top10 > 80:
                warnings.append("Top ten token accounts exceed 80% of supply.")
            warnings.append(
                "Holder concentration includes unidentified program and pool vaults."
            )
        except InfrastructureIndeterminateError as exc:
            infrastructure_errors.append(str(exc))
            unknown["holder_distribution"] = (
                "Largest-account evidence was unavailable from the configured RPC."
            )

        deployer_score = 50.0
        creator_score = 50.0
        deployer_data = {"resolved": False, "reason": unknown["deployer"]}
        creator_data = {"resolved": False, "reason": unknown["creator_risk"]}
        try:
            deployer_data, creator_data, provenance_facts = (
                self._deployer_and_creator_risk(mint, supply_raw=supply_raw)
            )
            facts.extend(provenance_facts)
            if deployer_data.get("resolved"):
                del unknown["deployer"]
                in_window = _safe_int(
                    deployer_data.get("prior_deployments_in_window")
                )
                hard_stop_window_seconds = 24 * 3600.0
                sample = deployer_data.get("prior_symbols_sample") or []
                in_hard_stop_window = (
                    sum(
                        1
                        for row in sample
                        if (time.time() - _safe_int(row.get("block_time")))
                        <= hard_stop_window_seconds
                    )
                    if sample
                    else in_window
                )
                if in_hard_stop_window >= 10:
                    deployer_score = 10.0
                    hard_stops.append(
                        self._hard_stop(
                            "creator_industrialized_deployment",
                            f"Deployer wallet launched {in_hard_stop_window} "
                            "tokens in the last 24h -- industrialized "
                            "deployment cadence.",
                        )
                    )
                elif in_window >= 5:
                    deployer_score = 40.0
                    warnings.append(
                        f"Deployer wallet has launched {in_window} tokens "
                        "in the last 72h."
                    )
                else:
                    deployer_score = 85.0
                    green.append(
                        "Deployer wallet shows no recent industrialized "
                        "launch pattern."
                    )
                if deployer_data.get("scan_degraded"):
                    warnings.append(
                        "Deployer history scan was partial (some lookups "
                        "failed) -- cadence figures are a floor, not a "
                        "confirmed total."
                    )
            if creator_data.get("resolved"):
                del unknown["creator_risk"]
                creator_pct = _safe_float(
                    creator_data.get("pct_total_supply"), None
                )
                if creator_pct is None:
                    creator_score = 65.0
                elif creator_pct > 5:
                    creator_score = 20.0
                    warnings.append(
                        f"Creator wallet holds {creator_pct:.1f}% of supply."
                    )
                elif creator_pct > 1:
                    creator_score = 50.0
                    warnings.append(
                        f"Creator wallet holds {creator_pct:.1f}% of supply."
                    )
                else:
                    creator_score = 80.0
                    green.append(
                        f"Creator wallet holds minimal supply "
                        f"({creator_pct:.2f}%)."
                    )
        except InfrastructureIndeterminateError as exc:
            infrastructure_errors.append(str(exc))

        pairs: list[dict] = []
        pair: dict | None = None
        dex_ok = False
        try:
            pairs = self.dexscreener.token_pairs(mint)
            dex_ok = True
            pair = self._market_pair(mint, pairs)
            facts.append(
                self._fact(
                    5,
                    "dexscreener",
                    {"endpoint": "token-pairs", "chain": "solana", "mint": mint},
                    {
                        "pair_count": len(pairs),
                        "selected_pair": (pair or {}).get("pairAddress"),
                        "selected_dex": (pair or {}).get("dexId"),
                        "liquidity_usd": ((pair or {}).get("liquidity") or {}).get("usd"),
                    },
                    slot_anchor,
                )
            )
        except InfrastructureIndeterminateError as exc:
            infrastructure_errors.append(str(exc))
            unknown["market"] = "DexScreener market evidence was unavailable."

        base = (pair or {}).get("baseToken") or {}
        quote = (pair or {}).get("quoteToken") or {}
        token_meta = base if base.get("address") == mint else quote
        liquidity_usd = _safe_float(
            ((pair or {}).get("liquidity") or {}).get("usd"), None
        )
        volume_24h = _safe_float(
            ((pair or {}).get("volume") or {}).get("h24"), None
        )
        market_cap = _safe_float((pair or {}).get("marketCap"), None)
        fdv = _safe_float((pair or {}).get("fdv"), None)
        created_ms = _safe_int((pair or {}).get("pairCreatedAt"), 0)
        market_age_seconds = (
            max(0, int(time.time() - created_ms / 1000))
            if created_ms > 0
            else None
        )
        price_usd = (
            _safe_float((pair or {}).get("priceUsd"), None)
            if base.get("address") == mint
            else None
        )
        if dex_ok and pair is None:
            hard_stops.append(
                self._hard_stop(
                    "no_secondary_market_detected",
                    "DexScreener returned no Solana market for this mint.",
                )
            )
        elif liquidity_usd is not None and liquidity_usd < self.policy.minimum_liquidity_usd:
            hard_stops.append(
                self._hard_stop(
                    "liquidity_below_5000_usd",
                    f"Observed liquidity is approximately ${liquidity_usd:,.0f}.",
                )
            )
        elif (
            liquidity_usd is not None
            and liquidity_usd < self.policy.caution_liquidity_usd
        ):
            warnings.append(
                f"Observed liquidity is limited at approximately ${liquidity_usd:,.0f}."
            )
        if market_age_seconds is not None and market_age_seconds < self.policy.new_market_seconds:
            warnings.append("The selected market is less than one hour old.")

        lp_lock_data = {
            "state": "custody_unverified",
            "amm_version": (pair or {}).get("dexId") or "unknown",
            "method": "generic_solana_pool_custody_unresolved",
            "locked": False,
            "withdrawal_verified": False,
            "withdrawable_pct": None,
        }
        lp_lock_score = 50.0
        pool_address = (pair or {}).get("pairAddress")
        if pool_address and str((pair or {}).get("dexId") or "").lower() == "pumpswap":
            try:
                pool_account = (
                    self.rpc.get_account_info(pool_address, encoding="base64") or {}
                )
                pool_owner = (pool_account.get("value") or {}).get("owner")
                facts.append(
                    self._fact(
                        10,
                        "solana_rpc",
                        {
                            "method": "getAccountInfo",
                            "purpose": "pumpswap_pool_custody",
                            "pool": pool_address,
                        },
                        {"owner": pool_owner},
                        slot_anchor,
                    )
                )
                if pool_owner == PUMP_AMM_PROGRAM_ID:
                    # PumpSwap pool vaults are PDAs owned by the AMM program
                    # itself -- no single wallet (not even the creator) can
                    # unilaterally withdraw the pool's liquidity. That's the
                    # Solana analog of an EVM LP token being locked/burned:
                    # proof custody sits with the protocol, not a person.
                    lp_lock_data = {
                        "state": "protocol_secured",
                        "amm_version": "pumpswap",
                        "method": "pool_account_owned_by_pump_amm_program",
                        "locked": True,
                        "withdrawal_verified": True,
                        "withdrawable_pct": 0.0,
                    }
                    lp_lock_score = 95.0
                    green.append(
                        "Liquidity custody protocol secured: pool is "
                        "owned by the canonical PumpSwap program."
                    )
                else:
                    lp_lock_data = {
                        "state": "custody_unexpected_owner",
                        "amm_version": "pumpswap",
                        "method": "pool_account_owned_by_pump_amm_program",
                        "locked": False,
                        "withdrawal_verified": False,
                        "withdrawable_pct": None,
                    }
                    lp_lock_score = 15.0
                    hard_stops.append(
                        self._hard_stop(
                            "lp_custody_unexpected_owner",
                            "The DexScreener-reported PumpSwap pool address "
                            "is not owned by the canonical PumpSwap program "
                            "on-chain.",
                        )
                    )
                del unknown["lp_lock"]
            except InfrastructureIndeterminateError as exc:
                infrastructure_errors.append(str(exc))
        # Any other dex (Raydium, Orca, etc.) or no market at all stays
        # honestly custody_unverified -- only the PumpSwap case above has a
        # concrete, cheap on-chain ownership check built for it so far.

        token_info: dict | None = None
        try:
            token_info = self.jupiter.token_info(mint)
            facts.append(
                self._fact(
                    6,
                    "jupiter",
                    {"endpoint": "tokens-search", "mint": mint},
                    {
                        key: (token_info or {}).get(key)
                        for key in (
                            "id",
                            "name",
                            "symbol",
                            "decimals",
                            "holderCount",
                            "liquidity",
                            "organicScore",
                            "organicScoreLabel",
                            "isVerified",
                            "mcap",
                            "firstPool",
                        )
                    },
                    slot_anchor,
                )
            )
        except InfrastructureIndeterminateError as exc:
            infrastructure_errors.append(str(exc))
            unknown["jupiter_metadata"] = "Jupiter token metadata was unavailable."

        execution: dict = {}
        if pair is not None:
            try:
                execution = self.jupiter.roundtrip(
                    mint, int(self.policy.quote_amount_sol * 1_000_000_000)
                )
                facts.append(
                    self._fact(
                        7,
                        "jupiter",
                        {
                            "endpoint": "swap-order",
                            "mint": mint,
                            "amount_sol": self.policy.quote_amount_sol,
                            "direction": "roundtrip",
                        },
                        {
                            "roundtrip_retention_pct": execution.get(
                                "roundtrip_retention_pct"
                            ),
                            "buy_price_impact_pct": (
                                (execution.get("buy") or {}).get("price_impact_pct")
                            ),
                            "buy_router": (execution.get("buy") or {}).get("router"),
                            "sell_router": (execution.get("sell") or {}).get("router"),
                        },
                        slot_anchor,
                    )
                )
                retention = _safe_float(
                    execution.get("roundtrip_retention_pct"), 0.0
                ) or 0.0
                impact = _safe_float(
                    (execution.get("buy") or {}).get("price_impact_pct"), None
                )
                if retention < self.policy.minimum_roundtrip_retention_pct:
                    hard_stops.append(
                        self._hard_stop(
                            "jupiter_roundtrip_retention_low",
                            f"A bounded Jupiter round trip retained only {retention:.1f}%.",
                        )
                    )
                if (
                    impact is not None
                    and impact > self.policy.maximum_buy_price_impact_pct
                ):
                    hard_stops.append(
                        self._hard_stop(
                            "jupiter_buy_price_impact_high",
                            f"Bounded buy price impact is {impact:.2f}%.",
                        )
                    )
                if not hard_stops:
                    green.append("A bounded two-way Jupiter route was observed.")
            except InfrastructureIndeterminateError as exc:
                infrastructure_errors.append(str(exc))
                unknown["honeypot_safety"] = (
                    "A two-way Jupiter route could not be verified."
                )

        top10 = _safe_float(
            concentration.get("top10_total_supply_pct"), None
        )
        holder_score = (
            max(10.0, min(75.0, 100.0 - top10))
            if top10 is not None
            else 50.0
        )
        security_score = 100.0
        if mint_authority:
            security_score -= 38
        if freeze_authority:
            security_score -= 38
        if risky_extensions:
            security_score -= min(45, 15 * len(risky_extensions))
        security_score = max(0.0, security_score)
        retention = _safe_float(
            execution.get("roundtrip_retention_pct"), None
        )
        honeypot_score = (
            max(5.0, min(95.0, retention))
            if retention is not None
            else 50.0
        )
        txns_h24 = ((pair or {}).get("txns") or {}).get("h24") or {}
        buys = _safe_int(txns_h24.get("buys"))
        sells = _safe_int(txns_h24.get("sells"))
        txn_total = buys + sells
        volume_score = (
            min(85.0, 35.0 + math.log10(max(1.0, volume_24h or 0.0)) * 9.0)
            if volume_24h is not None
            else 50.0
        )
        sentiment_score = (
            max(15.0, min(85.0, 50.0 + 35.0 * (buys - sells) / txn_total))
            if txn_total
            else 50.0
        )
        # Lightweight wash-trading heuristic from DexScreener's already-
        # fetched 24h buy/sell counts and USD volume -- deliberately not the
        # on-chain ping-pong/circular-transfer graph scan chainseer.py runs
        # for EVM (_detect_wash_trading), which needs per-wallet transfer
        # attribution across multiple block windows. Replicating that for
        # Solana would mean decoding a real window of transaction history
        # per public request, which this stateless, rate-limited endpoint
        # isn't budgeted for. This heuristic looks for the two cheapest,
        # DexScreener-visible tells instead: abnormally tiny average trade
        # size (bot-like microtransactions) and a suspiciously exact
        # buy/sell balance -- real organic markets are rarely both.
        wash_score = 50.0
        if txn_total >= 5:
            del unknown["wash_trading"]
            avg_trade_usd = (
                (volume_24h or 0.0) / txn_total if txn_total else None
            )
            balance_ratio = (
                abs(buys - sells) / txn_total if txn_total else 1.0
            )
            tiny_trades = avg_trade_usd is not None and avg_trade_usd < 2.0
            suspiciously_balanced = txn_total >= 20 and balance_ratio <= 0.05
            if tiny_trades and suspiciously_balanced:
                wash_score = 20.0
                warnings.append(
                    "Trading pattern shows both abnormally small average "
                    "trade size and a near-exact buy/sell balance -- "
                    "consistent with wash trading, not confirmed."
                )
            elif tiny_trades or suspiciously_balanced:
                wash_score = 40.0
                warnings.append(
                    "Trading pattern shows one wash-trading tell "
                    f"({'tiny average trade size' if tiny_trades else 'a near-exact buy/sell balance'})."
                )
            else:
                wash_score = 70.0
                green.append(
                    "No wash-trading tell detected in 24h buy/sell/volume pattern."
                )
        price_change = (pair or {}).get("priceChange") or {}
        change_h24 = _safe_float(price_change.get("h24"), None)
        trend_score = (
            max(15.0, min(85.0, 50.0 + change_h24))
            if change_h24 is not None
            else 50.0
        )
        factors = {
            "security": round(security_score, 1),
            "honeypot_safety": round(honeypot_score, 1),
            "liquidity": round(
                self._score_liquidity(liquidity_usd)
                if dex_ok
                else 50.0,
                1,
            ),
            "lp_lock": round(lp_lock_score, 1),
            "holder_distribution": round(holder_score, 1),
            "volume": round(volume_score, 1),
            "maturity": round(
                self._score_maturity(market_age_seconds)
                if dex_ok
                else 50.0,
                1,
            ),
            "creator_risk": round(creator_score, 1),
            "wash_trading": round(wash_score, 1),
            "deployer": round(deployer_score, 1),
            "sentiment": round(sentiment_score, 1),
            "trend": round(trend_score, 1),
        }
        score = round(sum(factors.values()) / len(factors), 1)
        if hard_stops:
            score = min(score, 35.0)

        essential_coverage = {
            "mint_state": True,
            "supply": supply_raw > 0,
            "holder_concentration": bool(concentration),
            "dexscreener_market": pair is not None,
            "jupiter_roundtrip": bool(execution),
        }
        completed = sum(bool(value) for value in essential_coverage.values())
        confidence = "MODERATE" if completed == len(essential_coverage) else "LIMITED"
        confidence_detail = (
            f"{completed}/{len(essential_coverage)} core Solana evidence groups. "
            "Creator attribution, pool-vault custody, and wash-trading remain "
            "unverified unless independently evidenced."
        )
        if hard_stops:
            risk_level = "High"
            action = "AVOID"
            recommendation = (
                "One or more deterministic hard stops are active. Avoid acting "
                "until fresh evidence proves those conditions have changed."
            )
        elif warnings or unknown or infrastructure_errors:
            risk_level = "Medium"
            action = "WATCHLIST"
            recommendation = (
                "No deterministic hard stop was confirmed, but material Solana "
                "custody, attribution, concentration, or execution unknowns "
                "remain. Treat this as watchlist evidence, not a safety verdict."
            )
        else:
            risk_level = "Low"
            action = "REVIEW"
            recommendation = (
                "No material condition was detected in the available evidence. "
                "Re-check immediately before acting because Solana state and "
                "market routes can change."
            )

        if infrastructure_errors:
            unknown["infrastructure"] = (
                f"{len(infrastructure_errors)} external evidence "
                "source(s) were indeterminate; this lowers confidence but "
                "does not count as token-negative evidence."
            )
        red_flags = [item["reason"] for item in hard_stops]
        yellow_flags = list(dict.fromkeys(warnings))
        coverage = {
            **essential_coverage,
            "creator_attribution": bool(deployer_data.get("resolved")),
            "liquidity_custody": lp_lock_data.get("state") != "custody_unverified",
            "wash_trading": txn_total >= 5,
            "slot_anchor": bool(slot_anchor),
        }
        poq_scores = {
            "coherence": 245,
            "relevance": 250,
            "novelty": 235,
            "consistency": 245 if completed >= 4 else 205,
            "depth": min(250, 175 + 10 * sum(bool(v) for v in coverage.values())),
            "covenant": 255,
        }
        name = (token_info or {}).get("name") or token_meta.get("name")
        symbol = (token_info or {}).get("symbol") or token_meta.get("symbol")
        report = {
            "token_address": mint,
            "token_name": name,
            "token_symbol": symbol,
            "chain_name": "Solana",
            "chain_id": "mainnet-beta",
            "explorer_url": SOLSCAN_TOKEN_URL + mint,
            "timestamp": _utc_now(),
            "coverage": coverage,
            "data": {
                "basic_info": {
                    "name": name,
                    "symbol": symbol,
                    "decimals": decimals,
                    "supply_raw": supply_raw,
                    "owner_program": owner_program,
                    "mint_authority": mint_authority,
                    "freeze_authority": freeze_authority,
                    "extensions": extensions,
                    "jupiter_verified": (token_info or {}).get("isVerified"),
                    "jupiter_holder_count": (token_info or {}).get("holderCount"),
                },
                "dex_pairs": {
                    "primary_price_usd": price_usd,
                    "market_cap": market_cap,
                    "market_cap_kind": (
                        "reported_market_cap"
                        if market_cap is not None and market_cap > 0
                        else "unavailable"
                    ),
                    "market_cap_source": "DexScreener",
                    "fdv": fdv,
                    "total_liquidity_usd": liquidity_usd,
                    "total_volume_24h": volume_24h,
                    "token_age_label": self._age_label(market_age_seconds),
                    "primary_amm_version": (pair or {}).get("dexId") or "unknown",
                    "primary_pair": (pair or {}).get("pairAddress"),
                    "primary_pair_url": (pair or {}).get("url"),
                    "txns_h24": txns_h24,
                    "price_change": price_change,
                },
                "lp_lock": lp_lock_data,
                "holder_concentration": concentration,
                "deployer": deployer_data,
                "creator": creator_data,
                "execution_evidence": execution,
                "source_code": {
                    "is_verified": None,
                    "reason": (
                        "SPL mints use shared token programs; contract-source "
                        "verification is not an ERC-20-equivalent signal."
                    ),
                },
            },
            "analysis": {
                "action_label": action,
                "risk_level": risk_level,
                "model_risk_level": risk_level,
                "legitimacy_score": score,
                "confidence_grade": confidence,
                "confidence": confidence_detail,
                "recommendation": recommendation,
                "hard_stop_overrides": hard_stops,
                "component_scores": factors,
                "red_flags": red_flags,
                "yellow_flags": yellow_flags,
                "green_flags": green,
                "uncertain_components": unknown,
                "extended_evidence": {
                    "social_attention": {
                        "status": "unmeasured",
                        "trust": "low",
                        "bounded_score": None,
                        "channels": [],
                        "dexscreener_boosts": 0,
                        "caveat": (
                            "Social evidence cannot override deterministic "
                            "Solana hard stops."
                        ),
                    },
                    "cross_chain": {
                        "status": "unmeasured",
                        "foreign_markets": [],
                        "verified_flow_count": 0,
                        "caveat": (
                            "Cross-chain identity and bridge flows are not "
                            "verified in this Solana release."
                        ),
                    },
                    "mev_exposure": {
                        "status": "measured" if execution else "unmeasured",
                        "risk_level": (
                            "High"
                            if any(
                                item["code"]
                                in {
                                    "jupiter_roundtrip_retention_low",
                                    "jupiter_buy_price_impact_high",
                                }
                                for item in hard_stops
                            )
                            else ("Low" if execution else "Unknown")
                        ),
                        "warnings": [
                            item["reason"]
                            for item in hard_stops
                            if item["code"].startswith("jupiter_")
                        ],
                        "scoring_scope": "execution_only",
                    },
                },
            },
            "provenance": {
                "fact_count": len(facts),
                "facts": facts,
                "block_pin": slot_anchor or account_slot,
                "anchor_type": "confirmed_slot_anchor",
                "anchor_caveat": (
                    "The scan is anchored to a confirmed starting slot. "
                    "Subsequent RPC and HTTP observations are content-hashed "
                    "but are not historical replays at one immutable slot."
                ),
            },
            "claim_evidence": {
                "deterministic_hard_stops": [
                    {"code": item["code"], "fact_ids": [fact["fact_id"] for fact in facts]}
                    for item in hard_stops
                ]
            },
            "infrastructure_indeterminate": list(
                dict.fromkeys(infrastructure_errors)
            ),
            "poq_scores": poq_scores,
        }
        report["data"]["entity_graph"] = build_solana_entity_graph(
            mint,
            report["data"],
            slot_anchor=slot_anchor or account_slot,
            facts=facts,
        )
        graph_ok, graph_reason = verify_entity_graph(
            report["data"]["entity_graph"]
        )
        if not graph_ok:
            raise RuntimeError(
                f"Entity evidence graph verification failed: {graph_reason}"
            )
        report["analysis"]["entity_insider_summary"] = (
            report["data"]["entity_graph"]["summary"]
        )
        self._seal_report(report)
        return report
