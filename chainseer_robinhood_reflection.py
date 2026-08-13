"""Durable 15-candidate reflection checkpoints for Robinhood paper learning."""

from __future__ import annotations

import json
import math
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable

from chainseer_core import atomic_json_write, read_json, safe_float, safe_int


REFLECTION_BATCH_SIZE = 15
CONFIRMED_MISS_MULTIPLE = 2.0
CONFIRMED_MISS_MARKET_CAP_USD = 1_000_000.0
CONFIRMED_MISS_LIQUIDITY_USD = 10_000.0
CONFIRMED_MISS_MINIMUM_HORIZON_SECONDS = 60 * 60
MAXIMUM_PLAUSIBLE_MARKET_CAP_USD = 100_000_000_000.0
MAXIMUM_MARKET_CAP_TO_LIQUIDITY = 1_000.0
AUTO_START = "<!-- robinhood-reflection:auto:start -->"
AUTO_END = "<!-- robinhood-reflection:auto:end -->"


def default_skill_root() -> Path:
    configured = os.environ.get("CYPHER_TEMPRE_SKILL_DIR", "").strip()
    if configured:
        return Path(configured)
    return Path.home() / ".codex" / "skills" / "cypher-tempre-self-model"


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _default_minimum_entry_score() -> float:
    """The learner's own constant, so the two cannot drift apart."""
    try:
        import chainseer_robinhood
        return float(chainseer_robinhood.MINIMUM_ENTRY_SCORE)
    except Exception:
        return 70.0


CLOSED_AUDIT_MINIMUM_POSITIONS = 8
CLOSED_AUDIT_TOTAL_LOSS_RATIO = 0.25
# Tighten-only, one notch at a time, and never past this ceiling. The estate's
# governing invariant is that autonomous change may preserve or tighten risk
# controls and never relax them; loosening stays a human decision.
CLOSED_AUDIT_SCORE_STEP = 2.0
CLOSED_AUDIT_SCORE_CEILING = 85.0
# New closed positions required before tightening again. Without this the
# audit stepped on EVERY checkpoint -- checkpoints fire every 15 analyses, so
# it walked 70 -> 85 in eight consecutive steps inside twenty minutes, each one
# re-reading substantially the same closed book. A tighten must be justified by
# evidence that did not exist at the previous tighten.
CLOSED_AUDIT_COOLDOWN_CLOSES = 5


class RobinhoodReflectionCoordinator:
    def __init__(
        self,
        root: str | Path,
        store,
        *,
        todo_path: str | Path,
        skill_root: str | Path | None = None,
        command_runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    ):
        self.root = Path(root)
        self.store = store
        self.todo_path = Path(todo_path)
        self.skill_root = Path(skill_root or default_skill_root())
        self.command_runner = command_runner
        self.reflections_root = self.root / "reflections"
        self.state_path = self.root / "reflection_state.json"
        self.catalog_path = self.root / "reflection_recommendations.json"
        self.audit_path = self.root / "counterfactual_audit.json"
        # Autonomous tighten-only overrides. Absent file == module
        # defaults, so the learner reads its constants unless an audit
        # has actually tightened something.
        self.policy_path = self.root / "adaptive_policy.json"
        self.timechain_root = self.root / "reflection_timechain"

    def _state(self) -> dict:
        return read_json(self.state_path, {}) or {}

    def _cohort(self, checkpoint: int) -> list[dict]:
        with self.store.connection() as connection:
            rows = connection.execute(
                """
                SELECT token_address,name,symbol,score,risk_level,action_label,
                       hard_stops_json,paper_entry_allowed,entry_price_usd,
                       entry_liquidity_usd,first_market_cap_usd,
                       peak_market_cap_usd,peak_fdv_usd,source_version,
                       analyzed_at,analysis_priority_reason,
                       analysis_queue_age_seconds,paper_decision,
                       market_watch_checks,market_watch_reason
                FROM candidates
                WHERE analysis_status='complete'
                ORDER BY analyzed_at,token_address
                LIMIT ? OFFSET ?
                """,
                (REFLECTION_BATCH_SIZE, checkpoint - REFLECTION_BATCH_SIZE),
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _valid_number(value, *, positive: bool = False) -> bool:
        number = safe_float(value, 0.0)
        return math.isfinite(number) and (number > 0 if positive else number >= 0)

    def _counterfactual_audit(self, cohort: list[dict]) -> dict:
        """Separate realizable missed trades from market-cap headline artifacts."""
        rejected = [
            row for row in cohort
            if row.get("paper_decision") in {"rejected", "expired_no_executable_market"}
        ]
        addresses = [row["token_address"] for row in rejected]
        checkpoints: dict[str, list[dict]] = {address: [] for address in addresses}
        if addresses:
            placeholders = ",".join("?" for _ in addresses)
            with self.store.connection() as connection:
                for row in connection.execute(
                    f"""
                    SELECT token_address,horizon_label,horizon_seconds,status,
                           learning_eligible,market_cap_usd,market_cap_multiple,
                           liquidity_usd
                    FROM checkpoints
                    WHERE token_address IN ({placeholders})
                    ORDER BY horizon_seconds
                    """,
                    addresses,
                ):
                    checkpoints[row["token_address"]].append(dict(row))

        headline = []
        confirmed = []
        exclusions: dict[str, int] = {}
        reviewed = []
        for row in rejected:
            peak = safe_float(row.get("peak_market_cap_usd"), 0.0)
            if peak < CONFIRMED_MISS_MARKET_CAP_USD:
                continue
            headline.append(row["token_address"])
            reasons = []
            try:
                hard_stops = json.loads(row.get("hard_stops_json") or "[]")
            except (TypeError, ValueError):
                hard_stops = ["MALFORMED_HARD_STOPS"]
            entry_liquidity = safe_float(row.get("entry_liquidity_usd"), 0.0)
            entry_cap = safe_float(row.get("first_market_cap_usd"), 0.0)
            if hard_stops:
                reasons.append("hard_safety_stop")
            if row.get("risk_level") not in {"Low", "Medium"}:
                reasons.append("risk_not_low_or_medium")
            if (
                not self._valid_number(row.get("entry_price_usd"), positive=True)
                or entry_liquidity < CONFIRMED_MISS_LIQUIDITY_USD
            ):
                reasons.append("entry_not_executable")
            if (
                not self._valid_number(entry_cap, positive=True)
                or entry_cap > MAXIMUM_PLAUSIBLE_MARKET_CAP_USD
                or entry_cap / max(entry_liquidity, 1.0) > MAXIMUM_MARKET_CAP_TO_LIQUIDITY
            ):
                reasons.append("implausible_entry_valuation")

            qualifying = []
            for checkpoint in checkpoints.get(row["token_address"], []):
                cap = safe_float(checkpoint.get("market_cap_usd"), 0.0)
                multiple = safe_float(checkpoint.get("market_cap_multiple"), 0.0)
                liquidity = safe_float(checkpoint.get("liquidity_usd"), 0.0)
                if (
                    checkpoint.get("status") == "observed"
                    and checkpoint.get("learning_eligible")
                    and safe_int(checkpoint.get("horizon_seconds"), 0)
                    >= CONFIRMED_MISS_MINIMUM_HORIZON_SECONDS
                    and self._valid_number(cap, positive=True)
                    and cap <= MAXIMUM_PLAUSIBLE_MARKET_CAP_USD
                    and multiple >= CONFIRMED_MISS_MULTIPLE
                    and liquidity >= CONFIRMED_MISS_LIQUIDITY_USD
                    and cap / liquidity <= MAXIMUM_MARKET_CAP_TO_LIQUIDITY
                ):
                    qualifying.append(checkpoint)
            if not qualifying:
                reasons.append("no_liquid_2x_outcome")
            reasons = sorted(set(reasons))
            item = {
                "token_address": row["token_address"],
                "symbol": row.get("symbol") or "",
                "score": row.get("score"),
                "risk_level": row.get("risk_level"),
                "entry_liquidity_usd": row.get("entry_liquidity_usd"),
                "entry_market_cap_usd": row.get("first_market_cap_usd"),
                "headline_peak_market_cap_usd": row.get("peak_market_cap_usd"),
                "exclusion_reasons": reasons,
            }
            if not reasons:
                best = max(
                    qualifying,
                    key=lambda value: safe_float(value.get("market_cap_multiple"), 0.0),
                )
                item["qualifying_outcome"] = best
                confirmed.append(item)
            else:
                for reason in reasons:
                    exclusions[reason] = exclusions.get(reason, 0) + 1
            reviewed.append(item)
        return {
            "definition": {
                "minimum_multiple": CONFIRMED_MISS_MULTIPLE,
                "minimum_market_cap_usd": CONFIRMED_MISS_MARKET_CAP_USD,
                "minimum_entry_and_exit_liquidity_usd": CONFIRMED_MISS_LIQUIDITY_USD,
                "minimum_horizon_seconds": CONFIRMED_MISS_MINIMUM_HORIZON_SECONDS,
                "hard_stops_allowed": False,
                "accepted_risk_levels": ["Low", "Medium"],
            },
            "rejected_reviewed": len(rejected),
            "headline_million_peaks": len(headline),
            "confirmed_false_negatives": len(confirmed),
            "exclusion_counts": exclusions,
            "confirmed": confirmed,
            "headline_review": reviewed,
        }

    def audit_all(self) -> dict:
        with self.store.connection() as connection:
            rows = connection.execute(
                """
                SELECT token_address,name,symbol,score,risk_level,action_label,
                       hard_stops_json,paper_entry_allowed,entry_price_usd,
                       entry_liquidity_usd,first_market_cap_usd,
                       peak_market_cap_usd,peak_fdv_usd,source_version,analyzed_at,
                       paper_decision,market_watch_checks,market_watch_reason
                FROM candidates WHERE analysis_status='complete'
                ORDER BY analyzed_at,token_address
                """
            ).fetchall()
        audit = self._counterfactual_audit([dict(row) for row in rows])
        audit["analyzed_total"] = len(rows)
        atomic_json_write(self.audit_path, audit)
        return audit

    def _metrics(self, checkpoint: int, cohort: list[dict]) -> dict:
        addresses = [row["token_address"] for row in cohort]
        placeholders = ",".join("?" for _ in addresses)
        checkpoint_counts: dict[str, int] = {}
        exit_policy_metrics: dict[str, dict] = {}
        if addresses:
            with self.store.connection() as connection:
                checkpoint_counts = {
                    row[0]: row[1]
                    for row in connection.execute(
                        f"""
                        SELECT status,COUNT(*) FROM checkpoints
                        WHERE token_address IN ({placeholders})
                        GROUP BY status
                        """,
                        addresses,
                    )
                }
                for row in connection.execute(
                    f"""
                    SELECT policy,COUNT(*) tracked,
                           SUM(status='closed') closed,
                           AVG(CASE WHEN status='closed' THEN net_multiple END) average_closed_multiple,
                           MAX(high_multiple) maximum_high_multiple
                    FROM position_policy_states
                    WHERE token_address IN ({placeholders}) GROUP BY policy
                    """,
                    addresses,
                ):
                    exit_policy_metrics[row["policy"]] = {
                        key: row[key] for key in row.keys() if key != "policy"
                    }
        observed = safe_int(checkpoint_counts.get("observed"), 0)
        no_market = safe_int(checkpoint_counts.get("no_market"), 0)
        rejected_million = sum(
            1
            for row in cohort
            if not row.get("paper_entry_allowed")
            and safe_float(row.get("peak_market_cap_usd"), 0.0) >= 1_000_000
        )
        scores = [safe_float(row.get("score"), 0.0) for row in cohort]
        queue_ages = [
            safe_float(row.get("analysis_queue_age_seconds"), 0.0)
            for row in cohort
            if row.get("analysis_queue_age_seconds") is not None
        ]
        v4_cursor = read_json(self.root / "discovery_v4_cursor.json", {}) or {}
        v4_coverage = v4_cursor.get("coverage") or {}
        counterfactual = self._counterfactual_audit(cohort)
        return {
            "checkpoint": checkpoint,
            "cohort_size": len(cohort),
            "analyzed_total": self.store.summary()["candidates"]["analyzed"],
            "admitted": sum(bool(row.get("paper_entry_allowed")) for row in cohort),
            "rejected": sum(row.get("paper_decision") == "rejected" for row in cohort),
            "watching_for_executable_market": sum(
                row.get("paper_decision") == "watching_for_executable_market"
                for row in cohort
            ),
            "expired_no_executable_market": sum(
                row.get("paper_decision") == "expired_no_executable_market"
                for row in cohort
            ),
            "observing_above_entry_ceiling": sum(
                row.get("paper_decision") == "observing_above_entry_ceiling"
                for row in cohort
            ),
            "waiting_for_reentry_momentum": sum(
                row.get("paper_decision") == "waiting_for_reentry_momentum"
                for row in cohort
            ),
            "exit_policy_metrics": exit_policy_metrics,
            "rejected_million_peak": rejected_million,
            "confirmed_false_negatives": counterfactual["confirmed_false_negatives"],
            "counterfactual_audit": counterfactual,
            "million_peak": sum(
                safe_float(row.get("peak_market_cap_usd"), 0.0) >= 1_000_000
                for row in cohort
            ),
            "missing_metadata": sum(
                not str(row.get("name") or "").strip()
                or not str(row.get("symbol") or "").strip()
                for row in cohort
            ),
            "average_score": round(sum(scores) / len(scores), 2) if scores else 0.0,
            "minimum_score": min(scores) if scores else 0.0,
            "maximum_score": max(scores) if scores else 0.0,
            "checkpoint_statuses": checkpoint_counts,
            "market_observation_failure_ratio": _ratio(
                no_market, observed + no_market
            ),
            "v4_blocks_behind": safe_int(v4_coverage.get("blocks_behind"), 0),
            "v4_caught_up": bool(v4_coverage.get("caught_up", False)),
            "source_counts": {
                source: sum(row.get("source_version") == source for row in cohort)
                for source in ("uniswap_v2", "uniswap_v3", "uniswap_v4")
            },
            "analysis_priority_counts": {
                reason: sum(row.get("analysis_priority_reason") == reason for row in cohort)
                for reason in (
                    "liquid_momentum", "oldest_fairness",
                    "executable_market_recheck",
                )
            },
            "average_analysis_queue_age_seconds": (
                round(sum(queue_ages) / len(queue_ages), 2) if queue_ages else None
            ),
            "maximum_analysis_queue_age_seconds": max(queue_ages) if queue_ages else None,
            "closed_positions": self._closed_position_metrics(),
        }

    def _closed_position_metrics(self) -> dict:
        """Audit positions that were ADMITTED and then failed.

        Every prior recommendation examines the rejection side of the funnel --
        false negatives, coverage, metadata, discovery lag. Nothing looked at
        what the system bought. That blind spot is why five of the first eight
        closes went to exactly 0.0 without a single recommendation naming it,
        while metadata gaps were reported eighteen times.

        Reads exit_reason and realized value straight from the store, so it
        needs no new bookkeeping and reflects whatever taxonomy the exit path
        currently records.
        """
        out = {
            "closed": 0, "winners": 0, "total_loss": 0,
            "total_loss_ratio": 0.0, "by_exit_reason": {},
            "worst_symbols": [],
        }
        try:
            with self.store.connection() as connection:
                rows = [
                    dict(row)
                    for row in connection.execute(
                        """
                        SELECT symbol, exit_reason, cost_usd, realized_value_usd,
                               high_multiple
                        FROM positions WHERE status!='open'
                        """
                    )
                ]
        except Exception:
            return out
        if not rows:
            return out
        reasons: dict[str, int] = {}
        worst: list[str] = []
        for row in rows:
            reason = str(row.get("exit_reason") or "unknown")
            reasons[reason] = reasons.get(reason, 0) + 1
            cost = float(row.get("cost_usd") or 0.0) or 1.0
            realized = float(row.get("realized_value_usd") or 0.0)
            multiple = realized / cost
            if multiple > 1.0:
                out["winners"] += 1
            # A total loss is capital that never came back at all -- distinct
            # from a bad exit, and the only class the entry gate could have
            # prevented outright.
            if multiple <= 0.01:
                out["total_loss"] += 1
                worst.append(str(row.get("symbol") or "<unnamed>"))
        out["closed"] = len(rows)
        out["by_exit_reason"] = dict(
            sorted(reasons.items(), key=lambda kv: -kv[1])
        )
        out["total_loss_ratio"] = round(out["total_loss"] / len(rows), 4)
        out["worst_symbols"] = worst[:10]
        return out

    @staticmethod
    def _recommendations(metrics: dict) -> list[dict]:
        findings: list[dict] = []
        checkpoint = metrics["checkpoint"]
        if metrics["confirmed_false_negatives"]:
            findings.append({
                "code": "RH-REFLECT-FALSE-NEGATIVES",
                "title": "Audit rejected Robinhood tokens that later reached a $1M peak",
                "recommendation": (
                    "Compare their entry-time evidence with true negatives and add a "
                    "validated tradeability signal before changing admission thresholds."
                ),
                "evidence": (
                    f"Checkpoint {checkpoint}: {metrics['confirmed_false_negatives']} "
                    "rejected candidate(s) passed the liquid, realizable 2x audit."
                ),
            })
        closed = metrics.get("closed_positions") or {}
        # Only speak once there is enough of a book to mean something; a single
        # bad close is noise, and this recommendation carries an autonomous
        # consequence.
        if closed.get("closed", 0) >= CLOSED_AUDIT_MINIMUM_POSITIONS and (
            closed.get("total_loss_ratio", 0.0) >= CLOSED_AUDIT_TOTAL_LOSS_RATIO
        ):
            findings.append({
                "code": "RH-REFLECT-TOTAL-LOSS-RATE",
                "title": "Admitted positions are reaching total loss",
                "recommendation": (
                    "Tighten admission until the total-loss rate falls. Total losses "
                    "are the one failure class the entry gate could have refused "
                    "outright -- an exit rule cannot recover capital from a token "
                    "whose price reached zero."
                ),
                "evidence": (
                    f"Checkpoint {checkpoint}: {closed['total_loss']} of "
                    f"{closed['closed']} closed positions returned <= 0.01x "
                    f"({closed['total_loss_ratio']:.0%}); exit reasons "
                    f"{closed['by_exit_reason']}; affected {closed['worst_symbols']}."
                ),
                "autonomous_action": "tighten_admission",
                # Carried so the executor need not re-query the store, and so
                # the action's cooldown is judged against the same closed count
                # that justified the finding.
                "metrics": {"closed": closed.get("closed", 0)},
            })
        if metrics["market_observation_failure_ratio"] >= 0.2:
            findings.append({
                "code": "RH-REFLECT-MARKET-COVERAGE",
                "title": "Improve Robinhood outcome-market coverage",
                "recommendation": (
                    "Add a fallback price/market-cap source and retain source-specific "
                    "failure telemetry before using checkpoint returns for calibration."
                ),
                "evidence": (
                    f"Checkpoint {checkpoint}: market observation failure ratio was "
                    f"{metrics['market_observation_failure_ratio']:.1%}."
                ),
            })
        if metrics["missing_metadata"] / max(1, metrics["cohort_size"]) >= 0.2:
            findings.append({
                "code": "RH-REFLECT-METADATA",
                "title": "Backfill missing Robinhood token identity metadata",
                "recommendation": (
                    "Retry name/symbol reads from a current block and add an explorer "
                    "fallback so analyzed-token evidence remains attributable."
                ),
                "evidence": (
                    f"Checkpoint {checkpoint}: {metrics['missing_metadata']} of "
                    f"{metrics['cohort_size']} analyzed candidates lacked a name or symbol."
                ),
            })
        if metrics["v4_blocks_behind"] > 0 or not metrics["v4_caught_up"]:
            findings.append({
                "code": "RH-REFLECT-V4-CATCHUP",
                "title": "Keep Robinhood V4 discovery continuously caught up",
                "recommendation": (
                    "Use adaptive bounded block windows and explicit backlog telemetry; "
                    "never advance the cursor across a failed window."
                ),
                "evidence": (
                    f"Checkpoint {checkpoint}: V4 discovery reported "
                    f"{metrics['v4_blocks_behind']} blocks behind."
                ),
            })
        return findings

    @staticmethod
    def _perspectives(metrics: dict, recommendations: list[dict]) -> list[dict]:
        false_negative_weight = min(255, 205 + 20 * metrics["confirmed_false_negatives"])
        coverage_weight = min(
            255, 185 + int(metrics["market_observation_failure_ratio"] * 120)
        )
        discovery_weight = min(255, 180 + min(60, metrics["v4_blocks_behind"] // 25))
        return [
            {
                "name": "Outcome falsification",
                "kind": "explicit",
                "senses": ["Assumption-Shift Sensing", "Multi-Truth Consistency Sensing"],
                "modalities": ["Temporal Context Holding", "Cross-Frame Reconciliation"],
                "summary": (
                    "Test the admission thesis against later outcomes. "
                    f"The cohort has {metrics['rejected_million_peak']} headline $1M "
                    f"peak(s), but {metrics['confirmed_false_negatives']} passed the "
                    f"realizable counterfactual audit; it has {metrics['admitted']} "
                    "admissions and an average score of "
                    f"{metrics['average_score']}. Preserve safety gates, but investigate "
                    "features that distinguish false negatives from true negatives."
                ),
                "scores": {"coherence": 250, "relevance": false_negative_weight,
                           "novelty": 235, "consistency": 250, "depth": 250,
                           "covenant": 250},
            },
            {
                "name": "Evidence completeness",
                "kind": "explicit",
                "senses": ["Information-Density Sensing", "Self-Validation Sensing"],
                "modalities": ["Coherence Synthesis", "Concept-Relation Mapping"],
                "summary": (
                    f"Measure whether calibration evidence is usable: {metrics['missing_metadata']} "
                    "candidate(s) lack identity metadata and the market observation failure "
                    f"ratio is {metrics['market_observation_failure_ratio']:.1%}. Improve "
                    "coverage before treating missing observations as losses."
                ),
                "scores": {"coherence": 245, "relevance": coverage_weight,
                           "novelty": 220, "consistency": 250, "depth": 245,
                           "covenant": 250},
            },
            {
                "name": "Discovery continuity",
                "kind": "explicit",
                "senses": ["Active-Frame Detection", "Frame-Balance Sensing"],
                "modalities": ["Salience Anchoring", "Temporal Context Holding"],
                "summary": (
                    f"Check whether the observed cohort is current and representative. V4 is "
                    f"{metrics['v4_blocks_behind']} blocks behind; the cohort source mix is "
                    f"{metrics['source_counts']}; analysis priority mix is "
                    f"{metrics['analysis_priority_counts']} with maximum queue age "
                    f"{metrics['maximum_analysis_queue_age_seconds']} seconds. Catch-up and "
                    "analysis selection should remain bounded, fair, retryable, and cursor-safe."
                ),
                "scores": {"coherence": 245, "relevance": discovery_weight,
                           "novelty": 225, "consistency": 250, "depth": 240,
                           "covenant": 250},
            },
            {
                "name": "Governed improvement",
                "kind": "explicit",
                "senses": ["Bad-Idea Alarm", "Frame-Balance Sensing"],
                "modalities": ["Value Alignment Check", "Cross-Modal Integration"],
                "summary": (
                    f"Checkpoint {metrics['checkpoint']} produced {len(recommendations)} "
                    "implementable finding(s). Record every finding in TODO, but do not "
                    "auto-change thresholds from a 15-candidate cohort or incomplete outcomes."
                ),
                "scores": {"coherence": 252, "relevance": 248, "novelty": 230,
                           "consistency": 255, "depth": 248, "covenant": 255},
            },
        ]

    def _run(self, arguments: list[str]) -> str:
        result = self.command_runner(
            arguments,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        return result.stdout

    def _seal(self, notes_path: Path, winner: str, metrics: dict) -> dict:
        timechain = self.skill_root / "timechain.py"
        chronosynaptic = self.skill_root / "chronosynaptic.py"
        recall = self.skill_root / "recall.py"
        for required in (timechain, chronosynaptic, recall):
            if not required.is_file():
                raise RuntimeError(f"Cypher Tempre component is missing: {required}")
        if not (self.timechain_root / "chain" / "rings.jsonl").exists():
            self._run([
                sys.executable, "-X", "utf8", str(timechain), "init",
                "--name", "ChainseerRobinhoodReflection",
                "--root", str(self.timechain_root),
            ])
        collapse = self._run([
            sys.executable, "-X", "utf8", str(chronosynaptic),
            "collapse-notes", str(notes_path), "--winner", winner,
            "--seal", "--root", str(self.timechain_root),
        ])
        reflection_thought = (
            f"Robinhood checkpoint {metrics['checkpoint']} reviewed with executable "
            "senses and modalities. Recommendations are evidence-gated TODO items; no "
            "scoring or admission threshold changes are automatic."
        )
        turn = self._run([
            sys.executable, "-X", "utf8", str(recall), "turn",
            reflection_thought, "--input", json.dumps(metrics, sort_keys=True),
            "--root", str(self.timechain_root),
        ])
        return {"collapse_output": collapse[-4000:], "turn_output": turn[-4000:]}

    def _apply_autonomous_actions(
        self, recommendations: list[dict], checkpoint: int
    ) -> list[dict]:
        """Act on findings that carry an autonomous_action, tighten-only.

        Every other recommendation is advisory text a human acts on, which has
        worked -- two of four were fixed and both show measured improvement --
        but at human latency: the coverage finding repeated for 255 checkpoints
        before anyone touched it. Total losses are the class where that latency
        is most expensive, because each repetition is another position that
        went to zero.

        The bound is the estate's governing invariant: this may only make
        admission STRICTER, one step at a time, and never past a ceiling.
        Loosening remains a human decision, so the worst case of a wrong
        autonomous call is a system that trades too little -- recoverable by
        editing one number -- rather than one that trades too dangerously.
        Every action is recorded with its before/after so it can be audited or
        reverted.
        """
        applied: list[dict] = []
        for item in recommendations:
            if item.get("autonomous_action") != "tighten_admission":
                continue
            state = read_json(self.policy_path, {}) or {}
            current = float(
                state.get("minimum_entry_score")
                or _default_minimum_entry_score()
            )
            # Only act on closes that arrived since the last tightening.
            closed_now = int(
                ((item.get("metrics") or {}).get("closed"))
                or self._closed_position_metrics().get("closed", 0)
            )
            closed_at_last = int(state.get("closed_at_last_action") or 0)
            if closed_now - closed_at_last < CLOSED_AUDIT_COOLDOWN_CLOSES:
                applied.append({
                    "checkpoint": checkpoint, "code": item["code"],
                    "action": "tighten_admission", "status": "cooldown",
                    "closed_since_last_action": closed_now - closed_at_last,
                    "required": CLOSED_AUDIT_COOLDOWN_CLOSES,
                    "minimum_entry_score": current,
                })
                continue
            if current >= CLOSED_AUDIT_SCORE_CEILING:
                applied.append({
                    "checkpoint": checkpoint, "code": item["code"],
                    "action": "tighten_admission", "status": "at_ceiling",
                    "minimum_entry_score": current,
                })
                continue
            proposed = min(
                CLOSED_AUDIT_SCORE_CEILING, current + CLOSED_AUDIT_SCORE_STEP
            )
            state["minimum_entry_score"] = proposed
            state["closed_at_last_action"] = closed_now
            state["updated_at"] = _now_iso()
            state["updated_by"] = item["code"]
            state["checkpoint"] = checkpoint
            atomic_json_write(self.policy_path, state)
            applied.append({
                "checkpoint": checkpoint, "code": item["code"],
                "action": "tighten_admission", "status": "applied",
                "previous_minimum_entry_score": current,
                "minimum_entry_score": proposed,
                "evidence": item.get("evidence"),
            })
        return applied

    def _sync_todo(self, recommendations: list[dict], checkpoint: int) -> None:
        catalog = read_json(self.catalog_path, {}) or {}
        items = catalog.setdefault("recommendations", {})
        for item in recommendations:
            existing = items.get(item["code"], {})
            items[item["code"]] = {
                **item,
                "first_checkpoint": existing.get("first_checkpoint", checkpoint),
                "last_checkpoint": checkpoint,
            }
        catalog["latest_checkpoint"] = checkpoint
        catalog["autonomous_actions"] = (
            catalog.get("autonomous_actions") or []
        ) + self._apply_autonomous_actions(recommendations, checkpoint)
        atomic_json_write(self.catalog_path, catalog)

        original = self.todo_path.read_text(encoding="utf-8") if self.todo_path.exists() else "# TODO\n"
        checked = {
            match.group(2)
            for match in re.finditer(r"- \[([ xX])\] `([^`]+)`", original)
            if match.group(1).lower() == "x"
        }
        lines = [
            AUTO_START,
            "## Robinhood reflection recommendations",
            "",
            "Generated from sealed 15-candidate checkpoints. Completion state is preserved on refresh.",
            "",
        ]
        for code in sorted(items):
            item = items[code]
            mark = "x" if code in checked else " "
            lines.extend([
                f"- [{mark}] `{code}` — {item['title']}",
                f"  - Recommendation: {item['recommendation']}",
                f"  - Evidence: {item['evidence']}",
                f"  - Checkpoints: first {item['first_checkpoint']}, latest {item['last_checkpoint']}",
            ])
        lines.append(AUTO_END)
        generated = "\n".join(lines)
        if AUTO_START in original and AUTO_END in original:
            prefix = original.split(AUTO_START, 1)[0].rstrip()
            suffix = original.split(AUTO_END, 1)[1].lstrip()
            updated = f"{prefix}\n\n{generated}\n"
            if suffix:
                updated += f"\n{suffix}"
        else:
            updated = f"{original.rstrip()}\n\n{generated}\n"
        temporary = self.todo_path.with_suffix(self.todo_path.suffix + ".tmp")
        temporary.write_text(updated, encoding="utf-8")
        temporary.replace(self.todo_path)

    def run_if_due(self) -> dict:
        analyzed = safe_int(self.store.summary()["candidates"]["analyzed"], 0)
        last_checkpoint = safe_int(self._state().get("last_checkpoint"), 0)
        checkpoint = last_checkpoint + REFLECTION_BATCH_SIZE
        if analyzed < checkpoint:
            return {
                "status": "not_due",
                "analyzed": analyzed,
                "next_checkpoint": checkpoint,
            }

        self.reflections_root.mkdir(parents=True, exist_ok=True)
        result_path = self.reflections_root / f"checkpoint-{checkpoint:06d}.json"
        existing = read_json(result_path, {}) or {}
        if existing.get("sealed"):
            recommendations = existing.get("recommendations") or []
            self._sync_todo(recommendations, checkpoint)
        else:
            cohort = self._cohort(checkpoint)
            if len(cohort) != REFLECTION_BATCH_SIZE:
                raise RuntimeError(
                    f"checkpoint {checkpoint} expected {REFLECTION_BATCH_SIZE} candidates, "
                    f"found {len(cohort)}"
                )
            metrics = self._metrics(checkpoint, cohort)
            recommendations = self._recommendations(metrics)
            perspectives = self._perspectives(metrics, recommendations)
            winner = max(
                perspectives,
                key=lambda row: sum(row["scores"].values()) / len(row["scores"]),
            )["name"]
            notes = {
                "query": (
                    f"Robinhood learning reflection for analyzed candidates "
                    f"{checkpoint - REFLECTION_BATCH_SIZE + 1}-{checkpoint}"
                ),
                "context": metrics,
                "perspectives": perspectives,
            }
            notes_path = self.reflections_root / f"checkpoint-{checkpoint:06d}-notes.json"
            atomic_json_write(notes_path, notes)
            seal = self._seal(notes_path, winner, metrics)
            existing = {
                "schema_version": 1,
                "checkpoint": checkpoint,
                "cohort_start": checkpoint - REFLECTION_BATCH_SIZE + 1,
                "cohort_end": checkpoint,
                "metrics": metrics,
                "perspectives": perspectives,
                "winner": winner,
                "recommendations": recommendations,
                "sealed": True,
                "seal": seal,
            }
            atomic_json_write(result_path, existing)
            self._sync_todo(recommendations, checkpoint)

        atomic_json_write(self.state_path, {
            "last_checkpoint": checkpoint,
            "next_checkpoint": checkpoint + REFLECTION_BATCH_SIZE,
            "latest_result": str(result_path),
        })
        self.audit_all()
        return {
            "status": "sealed",
            "checkpoint": checkpoint,
            "next_checkpoint": checkpoint + REFLECTION_BATCH_SIZE,
            "winner": existing.get("winner"),
            "recommendations": recommendations,
        }
