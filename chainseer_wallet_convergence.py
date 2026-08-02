"""Prospective wallet-accumulation registry and convergence detector.

Survivorship-bias-safe by construction (ring 100 design): wallets are
enrolled PROSPECTIVELY the first time they are observed as a holder of ANY
analyzed token, and EVERY token they are later observed holding is recorded
-- including the ones that go on to fail hard-stops. A wallet is only
labelled 'historically accurate' (never 'smart') once enough of its
observed positions have a known outcome AND its forward hit-rate clears a
floor. The convergence signal is strictly additive context: it can NEVER
override a hard-stop.

'Hit' definition: an observed position whose token later reached
evidence_state "complete_safe" (passed every hard-stop). This is an
ANALYSIS-OUTCOME oracle, not a price oracle -- it measures 'did the gate
clear this token', which is the signal the tracker exists to learn. Price
outcomes would introduce the very survivorship bias this design avoids.

Shared, dependency-light module (only chainseer_core's plain JSON/time
helpers) so it can back both the Solana paper-autotrader
(chainseer_solana.py, keyed on its own state root) and the stateless
public on-demand analyzers, each with their own independent state file --
they never share a registry, since the autotrader's bulk-scan history and
a public query's on-demand lookups are different lifecycles.
"""

from __future__ import annotations

from pathlib import Path

from chainseer_core import atomic_json_write as _atomic_json
from chainseer_core import read_json as _read_json
from chainseer_core import utc_now as _utc_now

SCHEMA_VERSION = 1


class WalletConvergenceTracker:
    def __init__(
        self,
        state_path: str | Path,
        *,
        minimum_observations_for_accuracy: int = 3,
        minimum_accuracy_pct: float = 50.0,
        convergence_minimum_accurate_wallets: int = 2,
    ):
        self.state_path = Path(state_path)
        self.minimum_observations_for_accuracy = max(
            1, int(minimum_observations_for_accuracy)
        )
        self.minimum_accuracy_pct = max(
            0.0, min(100.0, float(minimum_accuracy_pct))
        )
        self.convergence_minimum_accurate_wallets = max(
            1, int(convergence_minimum_accurate_wallets)
        )
        self.state = self._load()

    def _load(self) -> dict:
        state = _read_json(
            self.state_path,
            {
                "schema_version": SCHEMA_VERSION,
                "wallets": {},
                "updated_at": None,
            },
        )
        if not isinstance(state.get("wallets"), dict):
            state["wallets"] = {}
        return state

    def _save(self) -> None:
        self.state["updated_at"] = _utc_now()
        _atomic_json(self.state_path, self.state)

    def observe(
        self,
        wallet: str,
        mint: str,
        *,
        evidence_state: str | None,
        observed_at: str | None = None,
    ) -> None:
        """Record that ``wallet`` was observed holding ``mint``.

        Prospective enrollment: the wallet enters the registry on first sight
        regardless of whether the token later passes or fails -- this is what
        keeps the dataset free of winner-only selection bias.
        """
        if not wallet or not mint:
            return
        wallets = self.state["wallets"]
        entry = wallets.get(wallet)
        if entry is None:
            entry = {
                "first_observed_at": observed_at or _utc_now(),
                "positions": {},
            }
            wallets[wallet] = entry
        entry["positions"][mint] = {
            "evidence_state": evidence_state,
            "observed_at": observed_at or _utc_now(),
        }
        self._save()

    def regrade(self, mint: str, evidence_state: str) -> int:
        """Update the recorded outcome for ``mint`` across all holding wallets.

        Called when an analysis re-classifies a token; lets the hit-rate track
        the latest verdict rather than the first observation. Returns the count
        of positions updated.
        """
        updated = 0
        for entry in self.state["wallets"].values():
            pos = entry.get("positions", {}).get(mint)
            if pos and pos.get("evidence_state") != evidence_state:
                pos["evidence_state"] = evidence_state
                pos["regraded_at"] = _utc_now()
                updated += 1
        if updated:
            self._save()
        return updated

    def wallet_accuracy(self, wallet: str) -> dict:
        """Compute a wallet's forward hit-rate from observed positions.

        A 'hit' is a position whose token reached complete_safe. Wallets with
        fewer than the minimum observation count are 'unrated' -- they have not
        yet accumulated enough prospective data to judge.
        """
        entry = self.state["wallets"].get(wallet) or {}
        positions = entry.get("positions") or {}
        known = [
            p for p in positions.values()
            if p.get("evidence_state") is not None
        ]
        total = len(known)
        if total < self.minimum_observations_for_accuracy:
            return {
                "wallet": wallet,
                "rated": False,
                "observations": total,
                "hits": sum(1 for p in known if p["evidence_state"] == "complete_safe"),
                "hit_rate_pct": None,
                "label": "unrated",
            }
        hits = sum(1 for p in known if p["evidence_state"] == "complete_safe")
        hit_rate = 100.0 * hits / total
        accurate = hit_rate >= self.minimum_accuracy_pct
        return {
            "wallet": wallet,
            "rated": True,
            "observations": total,
            "hits": hits,
            "hit_rate_pct": round(hit_rate, 1),
            "label": "historically_accurate" if accurate else "historically_inaccurate",
        }

    def convergence_for(
        self, candidate_holders: list[dict]
    ) -> dict:
        """How many historically-accurate wallets also hold this token.

        ``candidate_holders`` is a list of holder dicts, each with an
        ``owner`` field. The result is strictly additive context -- the
        caller must NEVER use it to override a hard-stop.
        """
        accurate_wallets = []
        for holder in candidate_holders:
            owner = holder.get("owner")
            if not owner:
                continue
            acc = self.wallet_accuracy(owner)
            if acc["rated"] and acc["label"] == "historically_accurate":
                accurate_wallets.append(acc)
        converged = len(accurate_wallets) >= self.convergence_minimum_accurate_wallets
        return {
            "accurate_wallets_holding": len(accurate_wallets),
            "converged": converged,
            "wallets": [
                {
                    "wallet": w["wallet"],
                    "observations": w["observations"],
                    "hit_rate_pct": w["hit_rate_pct"],
                }
                for w in accurate_wallets
            ],
            "minimum_for_convergence": self.convergence_minimum_accurate_wallets,
            "caveat": (
                "Convergence is an additive correlation observation, not a "
                "safety signal. It must never override a hard-stop. Wallet "
                "accuracy is measured against analysis outcomes, not price."
            ),
        }

    def snapshot(self, limit: int = 20) -> dict:
        """Top historically-accurate wallets for dashboard / observability."""
        rated = []
        for wallet in self.state["wallets"]:
            acc = self.wallet_accuracy(wallet)
            if acc["rated"]:
                rated.append(acc)
        rated.sort(
            key=lambda w: (w["hit_rate_pct"] or 0, w["observations"]),
            reverse=True,
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "total_wallets_enrolled": len(self.state["wallets"]),
            "rated_wallets": len(rated),
            "top_accurate_wallets": rated[:limit],
            "updated_at": self.state.get("updated_at"),
        }
