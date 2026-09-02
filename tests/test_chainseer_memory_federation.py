import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from chainseer_memory import _load_module, _skill_dir
from chainseer_memory_federation import (
    FederatedMemoryCore,
    MemoryFederationError,
    MemorySource,
)
from chainseer_outcome_ledger import (
    analysis_evidence_binding,
    build_outcome_record,
)


TOKEN = "0x" + "c" * 40


class FederatedMemoryCoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.source_root = self.base / "robinhood_source"
        self.federation_root = self.base / "federation"
        self.timechain = _load_module(_skill_dir(), "timechain")
        self.source_tc = self.timechain.Timechain(self.source_root)
        self.source_tc.genesis(name="Robinhood producer")
        self.federation_tc = self.timechain.Timechain(self.federation_root)
        self.core = FederatedMemoryCore(
            self.federation_tc,
            self.federation_root,
            [MemorySource(
                "robinhood-learning", self.source_root, self.source_tc)],
        )

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def _provenance(pin, prefix):
        return {
            "block_pin": pin,
            "fact_count": 1,
            "facts": [{
                "fact_id": f"{prefix}0001",
                "source": "rpc",
                "query_hash": prefix.lower() * 64,
                "response_hash": str((int(pin) % 9) + 1) * 64,
                "block": pin,
                "fetched_at": "2026-09-02T08:00:00+00:00",
                "cache_hit": False,
            }],
        }

    def _seal_analysis(self, *, legacy=False):
        provenance = self._provenance(123, "a")
        payload = {
            "network": "robinhood",
            "chain_id": 4663,
            "token_address": TOKEN,
            "analysis_version": "federation-test-v1",
            "legitimacy_score": 81.0,
            "risk_level": "Low",
            "action_label": "WATCHLIST",
            "hard_stop_overrides": [],
            "component_scores": {"security": 84},
            "confidence_grade": "HIGH",
            "timestamp": "2026-09-02T08:00:01+00:00",
            "block_pin": 123,
            "provenance": provenance,
        }
        if not legacy:
            payload.update(analysis_evidence_binding(
                provenance, anchor_type="block_pin", anchor_value=123))
        return self.source_tc.seal("token_analysis", payload)

    def _seal_outcome(self, analysis):
        provenance = self._provenance(150, "b")
        record = build_outcome_record(
            analysis,
            {"rug_pull": False, "price_return_pct": 21.5},
            observed_at=datetime.now(timezone.utc).isoformat(),
            outcome_provenance=provenance,
            evidence_fact_ids=["b0001"],
        )
        return self.source_tc.seal(
            "robinhood_learning_outcome",
            {"summary": "Verified outcome", "outcome_record": record},
        )

    def test_ingests_complete_loop_and_returns_exact_citations(self):
        analysis = self._seal_analysis()
        self._seal_outcome(analysis)
        result = self.core.ingest("robinhood-learning", batch_size=1)
        self.assertEqual(result["records_ingested"], 2)
        self.assertEqual(result["records_by_kind"], {
            "analysis": 1, "outcome": 1})
        query = self.core.query("robinhood", TOKEN)
        self.assertEqual(
            {claim["category"] for claim in query["claims"]},
            {"analysis", "outcome"},
        )
        self.assertEqual(query["integrity"]["citation_coverage_pct"], 100.0)
        self.assertTrue(all(claim["citations"] for claim in query["claims"]))
        serialized = json.dumps(query)
        self.assertNotIn("query_hash", serialized)
        self.assertNotIn('"payload"', serialized)
        self.assertFalse(query["execution"]["signing"])
        self.assertFalse(query["execution"]["broadcast"])

    def test_ingest_is_idempotent_by_exact_source_ring(self):
        self._seal_analysis()
        first = self.core.ingest("robinhood-learning")
        height = self.federation_tc.height()
        second = self.core.ingest("robinhood-learning")
        self.assertEqual(first["records_ingested"], 1)
        self.assertEqual(second["records_ingested"], 0)
        self.assertTrue(second["idempotent_noop"])
        self.assertEqual(self.federation_tc.height(), height)

    def test_incomplete_legacy_analysis_is_not_federated(self):
        self._seal_analysis(legacy=True)
        result = self.core.ingest("robinhood-learning")
        self.assertEqual(result["records_ingested"], 0)
        self.assertEqual(self.core.query("robinhood", TOKEN)["claims"], [])

    def test_incomplete_noneligible_outcome_is_excluded_not_laundered(self):
        analysis = self._seal_analysis()
        record = build_outcome_record(
            analysis,
            {"rug_pull": False},
            observed_at=datetime.now(timezone.utc).isoformat(),
        )
        self.assertFalse(record["learning"]["eligible"])
        self.source_tc.seal(
            "robinhood_learning_outcome", {"outcome_record": record})
        result = self.core.ingest("robinhood-learning")
        self.assertEqual(result["records_by_kind"], {"analysis": 1})
        self.assertEqual(
            result["records_excluded"]["outcome_evidence_incomplete"], 1)

    def test_changed_source_history_fails_closed(self):
        self._seal_analysis()
        self.core.ingest("robinhood-learning")
        rings_path = self.source_tc.rings_path
        lines = rings_path.read_text(encoding="utf-8").splitlines()
        ring = json.loads(lines[-1])
        ring["ring_hash"] = "f" * 64
        lines[-1] = json.dumps(ring, sort_keys=True, separators=(",", ":"))
        rings_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        with self.assertRaises(MemoryFederationError):
            self.core.ingest("robinhood-learning")

    def test_query_reverifies_source_citation(self):
        self._seal_analysis()
        self.core.ingest("robinhood-learning")
        rings_path = self.source_tc.rings_path
        rings_path.write_text(
            rings_path.read_text(encoding="utf-8") + "{}\n",
            encoding="utf-8",
        )
        with self.assertRaises(MemoryFederationError):
            self.core.query("robinhood", TOKEN)


if __name__ == "__main__":
    unittest.main()
