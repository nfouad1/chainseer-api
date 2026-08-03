import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from chainseer_memory import (
    MemoryCore,
    MemoryCoreError,
    MemoryRecallEngine,
    MemoryRecoveryManager,
    _load_module,
    _skill_dir,
)
from chainseer_outcome_ledger import (
    analysis_evidence_binding,
    build_outcome_record,
)


TOKEN = "0x" + "a" * 40
OTHER_TOKEN = "0x" + "b" * 40


class MemoryCoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.root = self.base / "chainseer_chain"
        skill = _skill_dir()
        self.timechain_module = _load_module(skill, "timechain")
        self.epochs_module = _load_module(skill, "epochs")
        self.tc = self.timechain_module.Timechain(self.root)
        self.tc.genesis(name="Memory Core test")

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def provenance(pin=123):
        return {
            "block_pin": pin,
            "fact_count": 1,
            "facts": [
                {
                    "fact_id": "F0001",
                    "source": "rpc",
                    "query_hash": "1" * 64,
                    "response_hash": "2" * 64,
                    "block": pin,
                    "fetched_at": "2026-08-03T08:00:00+00:00",
                    "cache_hit": False,
                }
            ],
        }

    def seal_analysis(self, token=TOKEN, *, legacy=False, score=72.0):
        provenance = self.provenance()
        payload = {
            "network": "robinhood",
            "chain_id": 4663,
            "token_address": token,
            "analysis_version": "test-v1",
            "legitimacy_score": score,
            "risk_level": "Medium",
            "action_label": "WATCHLIST",
            "hard_stop_overrides": [],
            "component_scores": {"security": 80},
            "confidence_grade": "MODERATE",
            "timestamp": "2026-08-03T08:00:01+00:00",
            "block_pin": 123,
            "provenance": provenance,
            "evidence_state": "token_evidence",
        }
        if not legacy:
            payload.update(
                analysis_evidence_binding(
                    provenance, anchor_type="block_pin", anchor_value=123
                )
            )
        return self.tc.seal("token_analysis", payload)

    def seal_outcome(self, analysis):
        outcome_provenance = {
            "block_pin": 150,
            "fact_count": 1,
            "facts": [
                {
                    "fact_id": "O0001",
                    "source": "rpc",
                    "query_hash": "3" * 64,
                    "response_hash": "4" * 64,
                    "block": 150,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "cache_hit": False,
                }
            ],
        }
        record = build_outcome_record(
            analysis,
            {"rug_pull": False, "price_return_pct": 18.5},
            observed_at=datetime.now(timezone.utc).isoformat(),
            outcome_provenance=outcome_provenance,
            evidence_fact_ids=["O0001"],
        )
        return self.tc.seal(
            "analysis_outcome",
            {"summary": "Verified test outcome", "outcome_record": record},
        )

    def test_query_returns_only_exactly_cited_claims(self):
        self.seal_analysis(legacy=True)
        analysis = self.seal_analysis(score=74.0)
        self.seal_outcome(analysis)
        result = MemoryRecallEngine(self.tc, self.root).query(
            "robinhood", TOKEN, limit=10
        )
        categories = {claim["category"] for claim in result["claims"]}
        self.assertIn("latest_assessment", categories)
        self.assertIn("risk_history", categories)
        self.assertIn("outcomes", categories)
        self.assertEqual(result["integrity"]["citation_coverage_pct"], 100.0)
        self.assertTrue(all(claim["citations"] for claim in result["claims"]))
        self.assertTrue(
            any(
                item["reason"] == "legacy_analysis_not_evidence_bound_at_seal"
                for item in result["exclusions"]
            )
        )
        self.assertNotIn("query_hash", json.dumps(result))

    def test_subject_isolation_and_result_tamper_detection(self):
        self.seal_analysis(TOKEN)
        self.seal_analysis(OTHER_TOKEN)
        engine = MemoryRecallEngine(self.tc, self.root)
        result = engine.query(
            "robinhood", TOKEN, topics=["latest_assessment"], limit=1
        )
        self.assertEqual(len(result["claims"]), 1)
        self.assertNotIn(OTHER_TOKEN, json.dumps(result))
        result["claims"][0]["value"]["risk_level"] = "Low"
        ok, reason = engine.verify_result(result)
        self.assertFalse(ok)
        self.assertIn("hash mismatch", reason)

    def test_citation_proof_is_sanitized(self):
        analysis = self.seal_analysis()
        proof = MemoryRecallEngine(self.tc, self.root).citation_proof(
            analysis["index"]
        )
        self.assertEqual(proof["kind"], "analysis")
        self.assertEqual(proof["ring_hash"], analysis["ring_hash"])
        serialized = json.dumps(proof)
        self.assertNotIn("payload", serialized)
        self.assertNotIn("query_hash", serialized)

    def test_backup_restore_and_drill_are_nondestructive(self):
        self.seal_analysis()
        backup_root = self.base / "backups"
        manager = MemoryRecoveryManager(
            self.root,
            backup_root,
            tc=self.tc,
            timechain_module=self.timechain_module,
            epochs_module=self.epochs_module,
        )
        live_before = self.tc.height()
        backup = manager.create_backup()
        ok, report = manager.verify_backup(backup["path"])
        self.assertTrue(ok, report)
        restored = self.base / "restored"
        receipt = manager.restore(backup["path"], restored)
        self.assertTrue(receipt["chain_verified"])
        self.assertEqual(
            self.timechain_module.Timechain(restored).height(), live_before
        )
        drill = manager.drill()
        self.assertEqual(drill["status"], "passed")
        self.assertEqual(drill["recovery"]["rpo_rings"], 0)
        self.assertTrue(drill["recovery"]["deterministic_rebuild"])
        self.assertFalse(drill["live_root_modified"])
        self.assertEqual(self.tc.height(), live_before)

    def test_backup_tamper_and_unsafe_restore_are_refused(self):
        self.seal_analysis()
        manager = MemoryRecoveryManager(
            self.root,
            self.base / "backups",
            tc=self.tc,
            timechain_module=self.timechain_module,
            epochs_module=self.epochs_module,
        )
        backup = manager.create_backup()
        rings = Path(backup["path"]) / "chain" / "rings.jsonl"
        rings.write_text(
            rings.read_text(encoding="utf-8") + "{}\n", encoding="utf-8"
        )
        ok, report = manager.verify_backup(backup["path"])
        self.assertFalse(ok)
        self.assertIn("mismatch", report["error"])
        with self.assertRaises(MemoryCoreError):
            manager.restore(backup["path"], self.root)
        nonempty = self.base / "nonempty"
        nonempty.mkdir()
        (nonempty / "keep.txt").write_text("user data", encoding="utf-8")
        with self.assertRaises(MemoryCoreError):
            manager.restore(backup["path"], nonempty)

    def test_status_exposes_all_five_pillars_and_fail_closed_policy(self):
        self.seal_analysis()
        # Persist the derived projection so status can prove it matches.
        TemporalGraphStore = __import__(
            "chainseer_temporal_graph", fromlist=["TemporalGraphStore"]
        ).TemporalGraphStore
        TemporalGraphStore(self.root).rebuild(self.tc.load())
        core = MemoryCore(
            self.tc,
            self.root,
            backup_root=self.base / "backups",
            timechain_module=self.timechain_module,
            epochs_module=self.epochs_module,
        )
        status = core.status(cache_seconds=0)
        self.assertEqual(status["status"], "healthy")
        self.assertEqual(
            set(status["pillars"]),
            {
                "timechain_ledger",
                "entity_knowledge_graph",
                "pattern_faculty_store",
                "outcome_ledger",
                "query_recall_engine",
            },
        )
        self.assertTrue(
            status["pillars"]["pattern_faculty_store"]["tighten_only"]
        )
        self.assertFalse(status["execution"]["signing"])
        self.assertFalse(
            status["recovery"]["last_drill_freshness"]["current_head_match"]
        )


if __name__ == "__main__":
    unittest.main()
