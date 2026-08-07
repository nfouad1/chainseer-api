import copy
import json
import tempfile
import unittest
from pathlib import Path

from chainseer_entity_graph import (
    build_robinhood_entity_graph,
    canonical_hash as graph_hash,
)
from chainseer_temporal_graph import (
    TemporalGraphError,
    TemporalGraphStore,
    build_temporal_projection,
    subject_temporal_view,
    verify_temporal_projection,
)


TOKEN_A = "0x" + "1" * 40
TOKEN_B = "0x" + "2" * 40
INSIDER = "0x" + "a" * 40
PAIR_A = "0x" + "b" * 40
PAIR_B = "0x" + "c" * 40


def evm_graph(token, pair=PAIR_A, *, owner=INSIDER, holders=True, pin=100):
    return build_robinhood_entity_graph(
        token,
        {
            "goplus_security": {
                "owner_address": owner,
                "creator_address": INSIDER,
            },
            "blockscout_address": {"creator_address": INSIDER},
            "deployer": {"creator_address": INSIDER},
            "dex_pairs": {"primary_pair_address": pair},
            "lp_lock": {"state": "custody_unverified"},
            "source_code": {"implementations": []},
            "blockscout_holders": (
                {
                    "concentration_denominator_raw": 1000,
                    "holders": [
                        {"address": INSIDER, "balance_raw": "100"}
                    ],
                }
                if holders
                else {}
            ),
        },
        block_pin=pin,
    )


def ring(
    index,
    token,
    graph,
    *,
    score=80,
    evidence_state="token_evidence",
    pin=100,
):
    return {
        "index": index,
        "ring_type": "token_analysis",
        "timestamp": f"2026-01-01T00:0{index}:00+00:00",
        "ring_hash": f"{index + 1:064x}",
        "payload": {
            "network": "robinhood",
            "token_address": token,
            "timestamp": f"2026-01-01T00:0{index}:00+00:00",
            "legitimacy_score": score,
            "risk_level": "Low" if score >= 70 else "High",
            "action_label": "WATCHLIST" if score >= 70 else "AVOID",
            "component_scores": {"security": score, "liquidity": score - 5},
            "hard_stop_overrides": [] if score >= 70 else [{"code": "owner_risk"}],
            "evidence_state": evidence_state,
            "evidence_hash": "e" * 64,
            "anchor_type": "block_pin",
            "anchor_value": pin,
            "entity_graph_snapshot": graph,
        },
    }


class TemporalEntityGraphTests(unittest.TestCase):
    def test_tracks_appearance_change_and_defensible_disappearance(self):
        first = evm_graph(TOKEN_A, pin=100)
        changed = copy.deepcopy(first)
        owner_edge = next(
            value
            for value in changed["edges"]
            if value["relationship"] == "controls_contract"
        )
        owner_edge["confidence"] = "high"
        changed["anchor"]["value"] = 101
        changed["graph_hash"] = graph_hash(
            {key: value for key, value in changed.items() if key != "graph_hash"}
        )
        absent = evm_graph(TOKEN_A, owner=None, pin=102)

        projection = build_temporal_projection([
            ring(0, TOKEN_A, first, pin=100),
            ring(1, TOKEN_A, changed, score=75, pin=101),
            ring(2, TOKEN_A, absent, score=65, pin=102),
        ])
        view = subject_temporal_view(projection, "robinhood", TOKEN_A)
        control_id = next(
            identifier
            for identifier, value in projection["relationships"].items()
            if value["relationship"] == "controls_contract"
        )
        events = [
            value["event"]
            for value in view["relationship_events"]
            if value["relationship_id"] == control_id
        ]
        self.assertEqual(events, ["appeared", "changed", "disappeared"])
        self.assertEqual(
            projection["relationships"][control_id]["state"], "disappeared"
        )
        disappeared = next(
            value
            for value in view["relationship_events"]
            if value["event"] == "disappeared"
            and value["relationship_id"] == control_id
        )
        self.assertEqual(disappeared["pin"]["value"], 102)
        self.assertEqual(disappeared["analysis_ring"]["index"], 2)
        self.assertEqual(disappeared["evidence_hash"], "e" * 64)

    def test_partial_holder_coverage_never_claims_disappearance(self):
        first = evm_graph(TOKEN_A, holders=True, pin=100)
        absent = evm_graph(TOKEN_A, holders=False, pin=101)
        projection = build_temporal_projection([
            ring(0, TOKEN_A, first, pin=100),
            ring(1, TOKEN_A, absent, pin=101),
        ])
        holder = next(
            value
            for value in projection["relationships"].values()
            if value["relationship"] == "holds"
        )
        self.assertEqual(holder["state"], "unconfirmed")
        view = subject_temporal_view(projection, "robinhood", TOKEN_A)
        holder_events = [
            value["event"]
            for value in view["relationship_events"]
            if value["relationship_id"] == holder["id"]
        ]
        self.assertEqual(holder_events, ["appeared", "not_observed"])

    def test_exact_address_reuse_creates_cross_subject_link(self):
        projection = build_temporal_projection([
            ring(0, TOKEN_A, evm_graph(TOKEN_A, pair=PAIR_A, pin=100)),
            ring(1, TOKEN_B, evm_graph(TOKEN_B, pair=PAIR_B, pin=101), pin=101),
        ])
        insider = next(
            value
            for value in projection["entities"].values()
            if value["address"] == INSIDER
        )
        self.assertEqual(len(insider["subjects"]), 2)
        self.assertGreaterEqual(
            projection["summary"]["cross_subject_identity_count"], 1
        )
        view = subject_temporal_view(projection, "robinhood", TOKEN_A)
        shared = next(
            value
            for value in view["shared_entities"]
            if value["address"] == INSIDER
        )
        self.assertEqual(shared["subject_count"], 2)
        self.assertIn(f"robinhood:{TOKEN_B}", shared["other_subjects"])

    def test_risk_evolution_excludes_infrastructure_points_from_calibration(self):
        graph_100 = evm_graph(TOKEN_A, pin=100)
        graph_101 = evm_graph(TOKEN_A, pin=101)
        graph_102 = evm_graph(TOKEN_A, pin=102)
        projection = build_temporal_projection([
            ring(0, TOKEN_A, graph_100, score=80, pin=100),
            ring(
                1,
                TOKEN_A,
                graph_101,
                score=10,
                evidence_state="infrastructure_indeterminate",
                pin=101,
            ),
            ring(2, TOKEN_A, graph_102, score=70, pin=102),
        ])
        evolution = subject_temporal_view(
            projection, "robinhood", TOKEN_A
        )["risk_evolution"]
        self.assertEqual(evolution["observation_count"], 3)
        self.assertEqual(evolution["usable_score_count"], 2)
        self.assertEqual(evolution["infrastructure_indeterminate_count"], 1)
        self.assertEqual(evolution["first_score"], 80)
        self.assertEqual(evolution["current_score"], 70)
        self.assertEqual(evolution["total_score_delta"], -10)

    def test_legacy_summary_only_ring_keeps_score_without_relationship_claims(self):
        legacy_graph = evm_graph(TOKEN_A, pin=100)
        summary_only = {
            "graph_hash": legacy_graph["graph_hash"],
            "summary": legacy_graph["summary"],
            "signals": legacy_graph["signals"],
        }
        legacy = ring(0, TOKEN_A, summary_only, score=72, pin=100)
        legacy["payload"].pop("evidence_state")
        projection = build_temporal_projection([legacy])
        view = subject_temporal_view(projection, "robinhood", TOKEN_A)
        self.assertEqual(view["analysis_count"], 1)
        self.assertEqual(view["legacy_graph_observations"], 1)
        self.assertEqual(view["relationship_summary"]["known"], 0)
        self.assertEqual(view["risk_evolution"]["legacy_evidence_state_count"], 1)

    def test_store_roundtrip_and_rebuild_detect_projection_tampering(self):
        rings = [ring(0, TOKEN_A, evm_graph(TOKEN_A, pin=100), pin=100)]
        with tempfile.TemporaryDirectory() as temporary:
            store = TemporalGraphStore(temporary)
            projection = store.rebuild(rings)
            self.assertEqual(store.verify(rings), (True, "verified"))
            stored = json.loads(Path(store.path).read_text(encoding="utf-8"))
            stored["summary"]["subject_count"] = 999
            Path(store.path).write_text(json.dumps(stored), encoding="utf-8")
            self.assertIsNone(store.load())
            rebuilt = store.rebuild(rings)
            self.assertEqual(rebuilt["projection_hash"], projection["projection_hash"])
            self.assertEqual(
                verify_temporal_projection(rebuilt, rings), (True, "verified")
            )

    def test_incremental_refresh_matches_full_rebuild(self):
        first = ring(0, TOKEN_A, evm_graph(TOKEN_A, pin=100), pin=100)
        second = ring(1, TOKEN_A, evm_graph(TOKEN_A, pin=101), score=70, pin=101)
        with tempfile.TemporaryDirectory() as temporary:
            store = TemporalGraphStore(temporary)
            initial = store.refresh([first])
            self.assertEqual(initial["source_chain"]["analysis_ring_count"], 1)
            incremental = store.refresh([first, second])
            rebuilt = build_temporal_projection([first, second])
            self.assertEqual(
                incremental["projection_hash"], rebuilt["projection_hash"]
            )
            self.assertEqual(store.verify([first, second]), (True, "verified"))

    def test_malformed_analysis_ring_is_skipped_not_fatal(self):
        good = ring(0, TOKEN_A, evm_graph(TOKEN_A, pin=100), pin=100)
        malformed = {
            "index": 1,
            "ring_type": "base_launch_analysis",
            "timestamp": "2026-01-01T00:01:00+00:00",
            "ring_hash": "f" * 64,
            "payload": {"summary": "no candidate/subject in this payload"},
        }
        projection = build_temporal_projection([good, malformed])
        self.assertEqual(projection["summary"]["subject_count"], 1)
        self.assertEqual(projection["source_chain"]["analysis_ring_count"], 1)
        with tempfile.TemporaryDirectory() as temporary:
            store = TemporalGraphStore(temporary)
            store.rebuild([good])
            refreshed = store.refresh([good, malformed])
            self.assertEqual(refreshed["summary"]["subject_count"], 1)

    def test_trusted_single_ring_append_matches_full_projection(self):
        first = ring(0, TOKEN_A, evm_graph(TOKEN_A, pin=100), pin=100)
        second = ring(
            3,
            TOKEN_A,
            evm_graph(TOKEN_A, pin=101),
            score=70,
            pin=101,
        )
        with tempfile.TemporaryDirectory() as temporary:
            store = TemporalGraphStore(temporary)
            store.rebuild([first])
            incremental = store.append_analysis_ring(second)
            rebuilt = build_temporal_projection([first, second])
            self.assertEqual(
                incremental["projection_hash"], rebuilt["projection_hash"]
            )
            self.assertEqual(
                incremental["source_chain"]["analysis_ring_count"], 2
            )
            self.assertEqual(incremental["source_chain"]["head_index"], 3)

    def test_trusted_append_refuses_missing_projection(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = TemporalGraphStore(temporary)
            with self.assertRaisesRegex(
                TemporalGraphError, "projection_missing_or_invalid"
            ):
                store.append_analysis_ring(
                    ring(0, TOKEN_A, evm_graph(TOKEN_A, pin=100), pin=100)
                )


if __name__ == "__main__":
    unittest.main()
