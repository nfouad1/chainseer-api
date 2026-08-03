import unittest

from chainseer_outcome_ledger import (
    analysis_evidence_binding,
    analysis_reference_from_ring,
    build_outcome_record,
    verify_outcome_record,
    verify_outcome_rings,
)


def provenance(pin=123, *, slot=False):
    return {
        "block_pin": pin,
        "anchor_type": "confirmed_slot_anchor" if slot else "block_pin",
        "fact_count": 2,
        "facts": [
            {
                "fact_id": "F0000",
                "source": "rpc",
                "query_hash": "a" * 64,
                "response_hash": "b" * 64,
                "slot" if slot else "block": pin,
                "fetched_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "fact_id": "F0001",
                "source": "http",
                "query_hash": "c" * 64,
                "response_hash": "d" * 64,
                "slot" if slot else "block": pin,
                "fetched_at": "2026-01-01T00:00:01+00:00",
            },
        ],
    }


def evm_analysis_ring():
    evidence = provenance()
    binding = analysis_evidence_binding(
        evidence,
        anchor_type="block_pin",
        anchor_value=123,
    )
    return {
        "index": 7,
        "ring_type": "token_analysis",
        "ring_hash": "e" * 64,
        "timestamp": "2026-01-01T00:00:02+00:00",
        "payload": {
            "network": "base",
            "chain_id": 8453,
            "token_address": "0x" + "1" * 40,
            "analysis_version": "7.1",
            "risk_level": "Low",
            "legitimacy_score": 88,
            "provenance": evidence,
            **binding,
        },
    }


class OutcomeLedgerTests(unittest.TestCase):
    def test_outcome_binds_exact_analysis_evidence_and_block_pin(self):
        analysis = evm_analysis_ring()
        outcome = build_outcome_record(
            analysis,
            {"rug_pull": True, "price_return_pct": -95},
            observed_at="2026-01-02T00:00:00+00:00",
            outcome_provenance=provenance(456),
            evidence_fact_ids=["F0000", "F0001"],
        )
        reference = outcome["analysis_reference"]
        self.assertEqual(reference["ring"], 7)
        self.assertEqual(reference["ring_hash"], "e" * 64)
        self.assertEqual(
            reference["original_evidence_hash"],
            analysis["payload"]["evidence_hash"],
        )
        self.assertEqual(reference["anchor_type"], "block_pin")
        self.assertEqual(reference["anchor_value"], 123)
        self.assertTrue(outcome["learning"]["eligible"])
        self.assertEqual(
            outcome["learning"]["reason"],
            "analysis_and_outcome_evidence_hashes_complete",
        )
        self.assertTrue(verify_outcome_record(outcome, analysis)[0])

    def test_solana_reference_preserves_slot_pin(self):
        evidence = provenance(999, slot=True)
        binding = analysis_evidence_binding(
            evidence,
            anchor_type="slot_pin",
            anchor_value=999,
        )
        ring = {
            "index": 8,
            "ring_type": "solana_token_analysis",
            "ring_hash": "f" * 64,
            "timestamp": "2026-01-01T00:00:02+00:00",
            "payload": {
                "network": "solana",
                "mint": "So11111111111111111111111111111111111111112",
                "slot_anchor": 999,
                **binding,
            },
        }
        reference = analysis_reference_from_ring(ring)
        self.assertEqual(reference["anchor_type"], "slot_pin")
        self.assertEqual(reference["anchor_value"], 999)

    def test_tampered_analysis_manifest_is_rejected(self):
        analysis = evm_analysis_ring()
        analysis["payload"]["evidence_manifest"]["pin"]["value"] = 124
        with self.assertRaisesRegex(ValueError, "evidence hash"):
            analysis_reference_from_ring(analysis)

    def test_tampered_outcome_record_is_rejected(self):
        analysis = evm_analysis_ring()
        outcome = build_outcome_record(
            analysis,
            {"price_return_pct": 10},
            observed_at="2026-01-02T00:00:00+00:00",
        )
        outcome["market_outcomes"]["price_return_pct"] = 11
        ok, reason = verify_outcome_record(outcome, analysis)
        self.assertFalse(ok)
        self.assertIn("record hash mismatch", reason)

    def test_outcome_without_ground_truth_evidence_is_not_trainable(self):
        outcome = build_outcome_record(
            evm_analysis_ring(),
            {"price_return_pct": 10},
            observed_at="2026-01-02T00:00:00+00:00",
        )
        self.assertFalse(outcome["learning"]["eligible"])
        self.assertEqual(
            outcome["learning"]["reason"],
            "outcome_evidence_incomplete",
        )

    def test_ring_ledger_verifier_resolves_analysis_reference(self):
        analysis = evm_analysis_ring()
        outcome = build_outcome_record(
            analysis,
            {"honeypot_observed": True},
            observed_at="2026-01-02T00:00:00+00:00",
            outcome_provenance=provenance(456),
        )
        outcome_ring = {
            "index": 9,
            "ring_type": "analysis_outcome",
            "ring_hash": "1" * 64,
            "payload": {"outcome_record": outcome},
        }
        status = verify_outcome_rings([analysis, outcome_ring])
        self.assertTrue(status["ok"])
        self.assertEqual(status["checked"], 1)
        self.assertEqual(status["learning_eligible"], 1)


if __name__ == "__main__":
    unittest.main()
