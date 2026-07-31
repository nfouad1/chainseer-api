"""chainseer_entities.py — first-pass deployer/creator repeat-offender registry.

Scope, stated honestly: this is NOT funding-source graph tracing (following SOL/
ETH transfers back through intermediate wallets to a common CEX deposit address
or bundler). That is a real, materially harder feature -- it needs its own RPC
transaction-graph walk per chain and belongs in a dedicated follow-up. What this
module does is much narrower and immediately actionable: each chain adapter
already extracts and canonicality-checks a creator/deployer address per token
(chainseer_solana.py's Pump CreateEvent `creator` field, chainseer_pons.py's
factory-decoded `deployer` field). This registry remembers that address across
launches, per chain, so a new token from a wallet that has previously launched
a token with an adverse outcome (rug, honeypot, owner-privilege abuse) can be
flagged before its own evidence has even fully matured -- "this deployer has
done this before" is a strong, cheap signal that today's adapters throw away
after each analysis.

Design:
- Keyed by (chain, address.lower()). A Base address and a Solana address are
  different namespaces; no cross-chain identity is claimed or attempted.
- Append-only per entity: every launch and every later-known outcome is kept,
  never overwritten, so the record explains itself on inspection.
- Uses chainseer_core for the same atomic-write/BOM-tolerant-read/canonical-
  hash discipline as the rest of the project, so a torn write can't corrupt the
  registry and it composes with the existing Timechain-adjacent state files.
- Fail-open, like chainseer_alerts.py: a caller that forgets to record an
  outcome, or a corrupt registry file, must never block or crash the analysis
  pipeline the registry is trying to inform.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from chainseer_core import atomic_json_write, read_json, utc_now


class EntityRegistry:
    """Per-chain deployer/creator track record, backed by one JSON file.

    Not a hash-linked ledger like the Timechain -- this is a queryable index
    over facts the Timechain already sealed elsewhere, kept separately so a
    repeat-offender lookup is a single indexed read rather than a chain scan.
    """

    def __init__(self, root: str | Path, filename: str = "deployer_registry.json"):
        self.path = Path(root) / filename
        self._state: dict[str, Any] | None = None

    def _load(self) -> dict[str, Any]:
        if self._state is None:
            self._state = read_json(self.path, default={"version": 1, "entities": {}})
            self._state.setdefault("entities", {})
        return self._state

    def _save(self) -> None:
        if self._state is not None:
            atomic_json_write(self.path, self._state)

    @staticmethod
    def _key(chain: str, address: str) -> str:
        return f"{chain.strip().lower()}:{(address or '').strip().lower()}"

    def record_launch(
        self,
        *,
        chain: str,
        address: str,
        token_address: str,
        symbol: str = "",
        risk_level: str = "",
        score: float | None = None,
        hard_stops: list[str] | None = None,
    ) -> None:
        """Register that `address` launched `token_address` on `chain`.
        Idempotent per (chain, address, token_address): calling this again
        for a token already on record updates that token's latest-known
        snapshot rather than duplicating it, so a re-analysis doesn't inflate
        the deployer's apparent launch count.
        """
        if not address or not token_address:
            return
        state = self._load()
        key = self._key(chain, address)
        entity = state["entities"].setdefault(
            key,
            {
                "chain": chain.strip().lower(),
                "address": address,
                "first_seen_at": utc_now(),
                "tokens": {},
            },
        )
        entity["tokens"][token_address.lower()] = {
            "token_address": token_address,
            "symbol": symbol,
            "risk_level": risk_level,
            "score": score,
            "hard_stops": list(hard_stops or []),
            "adverse_outcome": entity["tokens"].get(token_address.lower(), {}).get(
                "adverse_outcome"
            ),
            "recorded_at": utc_now(),
        }
        entity["last_seen_at"] = utc_now()
        self._save()

    def record_outcome(
        self, *, chain: str, address: str, token_address: str, adverse: bool
    ) -> None:
        """Attach a later-known adverse/clean outcome to an already-recorded
        launch. No-ops quietly if the launch was never recorded (e.g. registry
        was added after the token launched) -- fail open, never raise into an
        outcome-collection pipeline over a missing history entry.
        """
        if not address or not token_address:
            return
        state = self._load()
        key = self._key(chain, address)
        entity = state["entities"].get(key)
        if entity is None:
            return
        record = entity["tokens"].get(token_address.lower())
        if record is None:
            return
        record["adverse_outcome"] = bool(adverse)
        record["outcome_recorded_at"] = utc_now()
        self._save()

    def track_record(self, *, chain: str, address: str) -> dict[str, Any]:
        """Return what's known about `address` on `chain` so far. Always
        returns a well-formed dict, even for an address never seen before
        (prior_launch_count=0), so callers can use this unconditionally
        without a None-check.
        """
        if not address:
            return self._empty_track_record()
        state = self._load()
        entity = state["entities"].get(self._key(chain, address))
        if entity is None:
            return self._empty_track_record()
        tokens = list(entity["tokens"].values())
        adverse = [t for t in tokens if t.get("adverse_outcome") is True]
        hard_stopped = [t for t in tokens if t.get("hard_stops")]
        return {
            "known": True,
            "chain": entity["chain"],
            "address": entity["address"],
            "first_seen_at": entity.get("first_seen_at"),
            "prior_launch_count": len(tokens),
            "prior_adverse_count": len(adverse),
            "prior_hard_stop_count": len(hard_stopped),
            "is_repeat_offender": len(adverse) > 0 or len(hard_stopped) > 0,
            "prior_tokens": [
                {
                    "token_address": t["token_address"],
                    "symbol": t.get("symbol"),
                    "adverse_outcome": t.get("adverse_outcome"),
                    "hard_stops": t.get("hard_stops") or [],
                }
                for t in tokens
            ],
        }

    @staticmethod
    def _empty_track_record() -> dict[str, Any]:
        return {
            "known": False,
            "chain": None,
            "address": None,
            "first_seen_at": None,
            "prior_launch_count": 0,
            "prior_adverse_count": 0,
            "prior_hard_stop_count": 0,
            "is_repeat_offender": False,
            "prior_tokens": [],
        }


def check_and_record(
    registry: EntityRegistry,
    *,
    chain: str,
    address: str,
    token_address: str,
    symbol: str = "",
    risk_level: str = "",
    score: float | None = None,
    hard_stops: list[str] | None = None,
) -> dict[str, Any]:
    """Convenience wrapper an adapter calls at analysis time: look up the
    deployer's track record for a REPEAT_DEPLOYER-style warning tag (computed
    from launches recorded before this one -- excludes the current token,
    since a token can't be a repeat of itself), then record this launch.
    Never raises: a broken registry should degrade to "unknown", not crash
    the analysis it's trying to enrich.
    """
    try:
        record = registry.track_record(chain=chain, address=address)
        registry.record_launch(
            chain=chain,
            address=address,
            token_address=token_address,
            symbol=symbol,
            risk_level=risk_level,
            score=score,
            hard_stops=hard_stops,
        )
        return record
    except Exception as exc:  # fail open, matching chainseer_alerts.py's contract
        return {**EntityRegistry._empty_track_record(), "error": type(exc).__name__}
