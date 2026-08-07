import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import chainseer
import chainseer_governance as governance


def effect_manifest(**overrides):
    value = {
        "schema_version": "1.0",
        "declared_effect": "observability_only",
        "authority": "cognitive_advisory_only",
        "score_effect": "none",
        "hard_stop_effect": "none",
        "admission_effect": "none",
        "execution_effect": "none",
        "threshold_changes": [],
    }
    value.update(overrides)
    return value


def relaxing_manifest():
    return effect_manifest(
        declared_effect="potentially_relaxing",
        authority="risk_policy",
        score_effect="may_raise_legitimacy",
        threshold_changes=[
            {
                "control": "min_trade_score",
                "current": 80,
                "proposed": 70,
                "tightening_direction": "increase",
            }
        ],
    )


class TightenOnlyAssessmentTests(unittest.TestCase):
    def test_cognitive_faculty_is_observability_only(self):
        result = governance.assess_effect_manifest(effect_manifest())
        self.assertEqual(result.classification, "observability_only")
        self.assertTrue(result.automatic_activation_allowed)
        self.assertFalse(result.human_override_required)

    def test_score_increase_and_lower_threshold_require_human_override(self):
        result = governance.assess_effect_manifest(relaxing_manifest())
        self.assertEqual(result.classification, "human_override_required")
        self.assertFalse(result.automatic_activation_allowed)
        self.assertTrue(result.human_override_required)
        self.assertTrue(
            any("may_raise_legitimacy" in item for item in result.relaxing_changes)
        )
        self.assertTrue(
            any("min_trade_score" in item for item in result.relaxing_changes)
        )

    def test_unknown_effect_fails_closed(self):
        value = effect_manifest()
        value.pop("hard_stop_effect")
        value["declared_effect"] = "potentially_relaxing"
        result = governance.assess_effect_manifest(value)
        self.assertTrue(result.human_override_required)
        self.assertIn("hard_stop_effect:unknown", result.unknown_changes)

    def test_live_execution_expansion_is_non_overridable(self):
        value = relaxing_manifest()
        value["execution_effect"] = "may_enable_or_broaden"
        result = governance.assess_effect_manifest(value)
        self.assertTrue(result.non_overridable)
        self.assertFalse(result.human_override_required)

    def test_calibration_checks_every_policy_dimension(self):
        current = {
            "min_trade_score": 80,
            "allowed_risk_levels": ["Low", "Medium"],
            "max_false_negative_rate": 0.05,
            "min_outcomes": 20,
            "max_permit_block_drift": 2,
            "max_quote_age_blocks": 2,
            "permit_ttl_seconds": 90,
        }
        tighter = {
            **current,
            "min_trade_score": 85,
            "allowed_risk_levels": ["Low"],
            "max_permit_block_drift": 1,
        }
        self.assertTrue(
            governance.assess_calibration_change(
                current, tighter
            ).automatic_activation_allowed
        )
        relaxed = {**tighter, "permit_ttl_seconds": 120}
        result = governance.assess_calibration_change(current, relaxed)
        self.assertFalse(result.automatic_activation_allowed)
        self.assertTrue(
            any("permit_ttl_seconds" in item for item in result.relaxing_changes)
        )


class GovernedPatternLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.skill = chainseer._get_skill_dir()
        chainseer._bootstrap_faculty_registry(self.root, self.skill)
        tc_module = chainseer._load_timechain_module(self.skill)
        self.epochs = chainseer._load_skill_module(self.skill, "epochs")
        self.tc = tc_module.Timechain(self.root)
        self.tc.genesis(name="GovernanceTest")
        self.epochs.seal_epoch(self.root, reason="test bootstrap")
        self.registry = governance.GovernedPatternRegistry(
            self.root, self.tc, self.epochs
        )

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def validation():
        return {
            "benchmark_hash": "b" * 64,
            "outcome_record_hashes": ["o" * 64],
            "sample_size": 25,
            "dangerous_false_negative_delta": -0.02,
            "false_positive_delta": 0.01,
        }

    def _propose(self, manifest):
        return self.registry.propose(
            {
                "name": "Test learned rule",
                "version": "0.1.0",
                "description": "A declarative test rule; never executable code.",
                "rule": {"when": "evidence_complete", "action": "warn"},
                "effect_manifest": manifest,
                "evidence": {"analysis_rings": [1, 2, 3]},
            }
        )

    def _validate(self, proposed):
        self.registry.transition(proposed["proposal_hash"], "shadow")
        return self.registry.transition(
            proposed["proposal_hash"], "validated", self.validation()
        )

    def test_tighten_only_pattern_needs_validation_and_creates_epoch(self):
        tightening = effect_manifest(
            declared_effect="tighten_only",
            authority="risk_policy",
            hard_stop_effect="add_only",
        )
        proposed = self._propose(tightening)
        with self.assertRaisesRegex(governance.GovernanceError, "invalid lifecycle"):
            self.registry.transition(proposed["proposal_hash"], "active")
        self._validate(proposed)
        active = self.registry.transition(proposed["proposal_hash"], "active")
        self.assertEqual(active["state"], "active")
        self.assertIsInstance(active["registry_epoch"], int)
        ok, report = governance.verify_governance_registry(self.root)
        self.assertTrue(ok, report)
        chain_ok, chain_report = self.tc.verify()
        self.assertTrue(chain_ok, chain_report)

    def test_relaxing_pattern_is_rejected_without_exact_human_override(self):
        proposed = self._propose(relaxing_manifest())
        self._validate(proposed)
        with self.assertRaisesRegex(
            governance.GovernanceError, "explicit human override"
        ):
            self.registry.transition(proposed["proposal_hash"], "active")
        invalid = governance.HumanOverride(
            approval_id="approval-1",
            approved_by="Alice Example",
            approved_at=datetime.now(timezone.utc).isoformat(),
            reason="Explicitly accept this bounded calibration tradeoff.",
            proposal_hash=proposed["proposal_hash"],
            confirmation="yes",
        )
        with self.assertRaisesRegex(governance.GovernanceError, "confirmation"):
            self.registry.transition(
                proposed["proposal_hash"], "active", human_override=invalid
            )

    def test_exact_human_override_is_bound_to_new_registry_epoch(self):
        proposed = self._propose(relaxing_manifest())
        self._validate(proposed)
        before = self.tc.height()
        override = governance.HumanOverride(
            approval_id="change-control-2026-001",
            approved_by="Alice Example",
            approved_at=datetime.now(timezone.utc).isoformat(),
            reason="Explicitly accept this bounded calibration tradeoff after review.",
            proposal_hash=proposed["proposal_hash"],
            confirmation=governance.OVERRIDE_CONFIRMATION,
        )
        active = self.registry.transition(
            proposed["proposal_hash"], "active", human_override=override
        )
        self.assertEqual(active["registry_epoch"], before)
        epoch = self.tc.load()[before]
        self.assertEqual(epoch["ring_type"], "epoch")
        transition = self.tc.load()[active["governance_ring"]]
        self.assertEqual(
            transition["payload"]["human_override"]["approval_id"],
            "change-control-2026-001",
        )
        ok, report = governance.verify_governance_registry(self.root)
        self.assertTrue(ok, report)

    def test_live_execution_pattern_cannot_be_human_overridden(self):
        manifest = relaxing_manifest()
        manifest["execution_effect"] = "may_enable_or_broaden"
        proposed = self._propose(manifest)
        self._validate(proposed)
        override = governance.HumanOverride(
            approval_id="change-control-2026-002",
            approved_by="Alice Example",
            approved_at=datetime.now(timezone.utc).isoformat(),
            reason="Attempt to enable execution should still be refused.",
            proposal_hash=proposed["proposal_hash"],
            confirmation=governance.OVERRIDE_CONFIRMATION,
        )
        with self.assertRaisesRegex(governance.GovernanceError, "non-overridable"):
            self.registry.transition(
                proposed["proposal_hash"], "active", human_override=override
            )

    def test_legacy_faculty_migration_is_epoch_bound_and_idempotent(self):
        grown_path = self.root / "registry" / "grown.json"
        grown_path.write_text(
            json.dumps(
                {
                    "registry": "grown",
                    "senses": [
                        {
                            "id": 900,
                            "name": "Legacy Observation",
                            "function": "Observe a bounded field.",
                            "effect": {
                                "type": "op",
                                "spec": {"primitive": "markers", "terms": ["field"]},
                            },
                        }
                    ],
                    "modalities": [],
                }
            ),
            encoding="utf-8",
        )
        # The fixture deliberately hand-writes a legacy registry, so re-anchoring
        # it is the "manual re-anchor after human review" case that
        # seal_epoch(accept_current=True) exists for. Production paths must NOT
        # use this -- they authorize via begin_mutation() instead.
        try:
            self.epochs.seal_epoch(
                self.root, reason="legacy fixture", accept_current=True
            )
        except TypeError:
            # Skill builds predating the accept_current gate have no such
            # kwarg -- and no mismatch guard needing satisfaction either. CI
            # pins an older Cypher Tempre commit than a developer machine may
            # have installed, so this fixture must work against both.
            self.epochs.seal_epoch(self.root, reason="legacy fixture")
        first = self.registry.migrate_cognitive_faculties()
        second = self.registry.migrate_cognitive_faculties()
        self.assertTrue(first["changed"])
        self.assertEqual(first["faculty_count"], 1)
        self.assertFalse(second["changed"])
        ok, report = governance.verify_governance_registry(self.root)
        self.assertTrue(ok, report)


class FacultyPackGovernanceTests(unittest.TestCase):
    def test_production_pack_is_fully_governed_and_hash_valid(self):
        path = (
            Path(chainseer.__file__).resolve().parent
            / "faculties"
            / "chainseer-production-v1.json"
        )
        pack = json.loads(path.read_text(encoding="utf-8"))
        result = governance.validate_faculty_pack_governance(pack)
        self.assertTrue(result["automatic_activation_allowed"])
        self.assertEqual(len(result["faculties"]), 12)
        faculties = chainseer._load_skill_module(
            chainseer._get_skill_dir(), "faculties"
        )
        self.assertEqual(faculties.pack_hash(pack), pack["pack_sha256"])

    def test_pack_without_effect_contract_is_rejected(self):
        pack = {
            "faculties": [
                {"kind": "sense", "name": "Ungoverned", "function": "Observe"}
            ]
        }
        with self.assertRaises(governance.GovernanceError):
            governance.validate_faculty_pack_governance(pack)


if __name__ == "__main__":
    unittest.main()
