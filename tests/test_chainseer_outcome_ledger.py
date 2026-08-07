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


class LaunchAdapterRingRecognitionTests(unittest.TestCase):
    """Pons and Solana together produced 6,912 analyses the ledger refused to
    read. Recognising them makes them citable; it must not make them look
    better-evidenced than they are.
    """

    @staticmethod
    def _pons_ring(**decision_overrides):
        decision = {
            "token_address": "0xC4Cb8A0167c77E36194f6affB6b71D931FAb62c0",
            "block_pin": 4242,
            "provenance": provenance(4242),
            "risk_level": "Low",
        }
        decision.update(decision_overrides)
        return {
            "index": 11,
            "ring_type": "pons_launch_analysis",
            "ring_hash": "a" * 64,
            "timestamp": "2026-01-01T00:00:00+00:00",
            "payload": {
                "chain_id": 4663,
                "protocol": "pons",
                "candidate": {"token_address": decision["token_address"]},
                "decision": decision,
            },
        }

    # Production shape: decision["mint"] is the mint ACCOUNT object, not the
    # address. The original fixture used a string here, which is why the
    # subject-extraction bug passed its tests while every real ring was keyed
    # by a metadata blob.
    MINT_ACCOUNT = {
        "owner_program": "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        "decimals": 6,
        "supply_raw": 1000000000000000,
        "mint_authority": None,
        "freeze_authority": None,
    }

    @staticmethod
    def _solana_ring(**payload_overrides):
        payload = {
            "candidate": {"mint": "81Z3jR9J9sNZMgot5zSnRNj4AcjH9WtcbG4qLERspump",
                          "slot": 777},
            "decision": {
                "mint": LaunchAdapterRingRecognitionTests.MINT_ACCOUNT,
                "risk_level": "High",
            },
        }
        payload.update(payload_overrides)
        return {
            "index": 12,
            "ring_type": "solana_launch_analysis",
            "ring_hash": "b" * 64,
            "timestamp": "2026-01-01T00:00:01+00:00",
            "payload": payload,
        }

    def test_pons_analysis_is_recognised(self):
        reference = analysis_reference_from_ring(self._pons_ring())
        # Pons runs ON Robinhood Chain, so it shares that subject namespace
        # with general token_analysis rather than inventing its own.
        self.assertEqual(reference["network"], "robinhood")
        self.assertEqual(
            reference["subject"], "0xC4Cb8A0167c77E36194f6affB6b71D931FAb62c0"
        )
        self.assertEqual(reference["anchor_type"], "block_pin")
        self.assertEqual(reference["anchor_value"], 4242)

    def test_pons_provenance_is_read_from_the_decision(self):
        # Pons seals provenance under decision, not at the payload root. Read
        # from the root and every Pons ring would look evidence-less.
        reference = analysis_reference_from_ring(self._pons_ring())
        self.assertTrue(reference["evidence_complete"])
        self.assertEqual(reference["evidence_fact_count"], 2)

    def test_solana_launch_analysis_pins_to_a_slot_not_a_block(self):
        reference = analysis_reference_from_ring(self._solana_ring())
        self.assertEqual(reference["network"], "solana")
        self.assertEqual(reference["anchor_type"], "slot_pin")
        self.assertEqual(reference["anchor_value"], 777)

    def test_solana_launch_shares_the_namespace_of_solana_token_analysis(self):
        # One mint analysed by both the public product and the autotrader is
        # ONE subject; splitting it would hide cross-system agreement.
        mint = "So11111111111111111111111111111111111111112"
        launch = analysis_reference_from_ring(
            self._solana_ring(
                candidate={"mint": mint, "slot": 5},
                decision={"mint": self.MINT_ACCOUNT},
            )
        )
        binding = analysis_evidence_binding(
            provenance(5, slot=True), anchor_type="slot_pin", anchor_value=5
        )
        token = analysis_reference_from_ring({
            "index": 13,
            "ring_type": "solana_token_analysis",
            "ring_hash": "c" * 64,
            "timestamp": "2026-01-01T00:00:02+00:00",
            "payload": {"network": "solana", "mint": mint,
                        "slot_anchor": 5, **binding},
        })
        self.assertEqual(launch["network"], token["network"])
        self.assertEqual(launch["subject"], token["subject"])

    def test_mint_account_object_is_never_used_as_the_subject(self):
        """decision["mint"] is the mint ACCOUNT, not the address.

        It is a dict and therefore truthy, so `decision.get("mint") or
        candidate.get("mint")` silently keyed every one of 6,143 production
        rings by a metadata blob instead of a mint address -- and they could
        never unify with solana_token_analysis, which was the entire reason
        for sharing the "solana" namespace.
        """
        reference = analysis_reference_from_ring(self._solana_ring())
        self.assertIsInstance(reference["subject"], str)
        self.assertEqual(
            reference["subject"], "81Z3jR9J9sNZMgot5zSnRNj4AcjH9WtcbG4qLERspump"
        )

    def test_subject_is_absent_rather_than_a_blob_when_no_address_exists(self):
        ring = self._solana_ring(
            candidate={"slot": 5}, decision={"mint": self.MINT_ACCOUNT}
        )
        self.assertIsNone(analysis_reference_from_ring(ring)["subject"])

    def test_legacy_rings_are_labelled_legacy_not_sealed(self):
        # These 6,912 rings predate evidence manifests. Recognising them must
        # not launder them into looking sealed-at-analysis.
        for ring in (self._pons_ring(), self._solana_ring()):
            reference = analysis_reference_from_ring(ring)
            self.assertEqual(
                reference["binding_state"], "derived_from_legacy_analysis_ring"
            )

    def test_missing_provenance_is_reported_as_incomplete_not_invented(self):
        ring = self._solana_ring()          # no provenance anywhere
        reference = analysis_reference_from_ring(ring)
        self.assertFalse(reference["evidence_complete"])
        self.assertEqual(reference["evidence_fact_count"], 0)


if __name__ == "__main__":
    unittest.main()


class OutcomeTimelinessTests(unittest.TestCase):
    """An outcome is only evidence for the horizon it CLAIMS to measure.

    Measured on the live Base ledger: 264 of 785 completed checkpoints (34%)
    were observed more than a day late after a stalled learner was restarted
    and its backlog drained -- the worst 12.32 days late while still labelled
    horizon=1h. Nothing downstream could distinguish those from on-time
    records, because learning eligibility only checked evidence hashes.
    """

    ANALYSIS_TIME = "2026-01-01T00:00:02+00:00"

    def _record(self, horizon_seconds, observed_at):
        return build_outcome_record(
            evm_analysis_ring(),
            {"price_return_pct": -12, "horizon_seconds": horizon_seconds},
            observed_at=observed_at,
            outcome_provenance=provenance(456),
            evidence_fact_ids=["F0000", "F0001"],
        )

    def test_on_time_outcome_is_learning_eligible(self):
        # 1h horizon observed ~1h after the analysis ring.
        record = self._record(3600, "2026-01-01T01:00:02+00:00")
        self.assertTrue(record["learning"]["eligible"])
        self.assertTrue(record["timing"]["within_tolerance"])
        self.assertAlmostEqual(record["timing"]["lateness_seconds"], 0.0, places=1)

    def test_slightly_late_outcome_is_still_eligible(self):
        # 5 minutes past a 1h horizon: ordinary scheduler jitter.
        record = self._record(3600, "2026-01-01T01:05:02+00:00")
        self.assertTrue(record["learning"]["eligible"])
        self.assertTrue(record["timing"]["within_tolerance"])

    def test_badly_late_outcome_is_excluded_from_learning(self):
        # The real incident: labelled 1h, observed 12.32 days later.
        record = self._record(3600, "2026-01-13T07:40:02+00:00")
        self.assertFalse(record["learning"]["eligible"])
        self.assertEqual(
            record["learning"]["reason"], "outcome_observed_too_late_for_its_horizon"
        )
        self.assertFalse(record["timing"]["within_tolerance"])
        self.assertGreater(record["timing"]["lateness_seconds"], 11 * 86400)

    def test_long_horizon_tolerance_scales_and_does_not_fail_on_jitter(self):
        # 6h late on a 7d horizon is proportionally trivial and must pass.
        record = self._record(7 * 86400, "2026-01-08T06:00:02+00:00")
        self.assertTrue(record["learning"]["eligible"])
        self.assertTrue(record["timing"]["within_tolerance"])

    def test_record_is_still_sealed_and_verifiable_when_late(self):
        """Late outcomes stay in the ledger -- excluded from learning, not
        discarded, so the history remains auditable."""
        analysis = evm_analysis_ring()
        record = self._record(3600, "2026-01-13T07:40:02+00:00")
        ok, reason = verify_outcome_record(record, analysis)
        self.assertTrue(ok, reason)

    def test_outcome_without_declared_horizon_is_not_penalised(self):
        """Lateness is unknowable without a nominal horizon -- report None
        rather than silently passing it off as on time or failing it."""
        record = build_outcome_record(
            evm_analysis_ring(),
            {"rug_pull": True},
            observed_at="2026-02-01T00:00:00+00:00",
            outcome_provenance=provenance(456),
            evidence_fact_ids=["F0000", "F0001"],
        )
        self.assertIsNone(record["timing"]["within_tolerance"])
        self.assertTrue(record["learning"]["eligible"])
