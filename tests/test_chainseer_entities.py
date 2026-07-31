import json
import tempfile
import unittest
from pathlib import Path

import chainseer_entities as entities


class EntityRegistryTests(unittest.TestCase):
    def test_unknown_address_returns_well_formed_empty_record(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            record = registry.track_record(chain="base", address="0xabc")
        self.assertFalse(record["known"])
        self.assertEqual(record["prior_launch_count"], 0)
        self.assertFalse(record["is_repeat_offender"])

    def test_empty_address_is_a_noop_not_an_error(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            record = registry.track_record(chain="base", address="")
        self.assertFalse(record["known"])

    def test_first_launch_is_recorded_and_visible_on_next_lookup(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            registry.record_launch(
                chain="solana", address="Creator111", token_address="Mint111",
                symbol="FOO", risk_level="Low", score=90,
            )
            record = registry.track_record(chain="solana", address="Creator111")
        self.assertTrue(record["known"])
        self.assertEqual(record["prior_launch_count"], 1)
        self.assertFalse(record["is_repeat_offender"])

    def test_address_lookup_is_case_insensitive(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            registry.record_launch(chain="base", address="0xABCDEF", token_address="0x111")
            record = registry.track_record(chain="base", address="0xabcdef")
        self.assertTrue(record["known"])

    def test_chains_are_isolated_namespaces(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            registry.record_launch(chain="base", address="0xSAME", token_address="0x111")
            solana_record = registry.track_record(chain="solana", address="0xSAME")
        self.assertFalse(solana_record["known"])

    def test_adverse_outcome_marks_repeat_offender_on_next_launch(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            registry.record_launch(
                chain="pons", address="0xDEP", token_address="0xTOKEN1",
                symbol="RUG1", risk_level="Low", score=85,
            )
            registry.record_outcome(
                chain="pons", address="0xDEP", token_address="0xTOKEN1", adverse=True
            )
            # A second, later launch from the same deployer should now show
            # up as a repeat offender based on the FIRST token's outcome.
            record = registry.track_record(chain="pons", address="0xDEP")
        self.assertTrue(record["is_repeat_offender"])
        self.assertEqual(record["prior_adverse_count"], 1)
        self.assertEqual(record["prior_launch_count"], 1)

    def test_hard_stops_alone_also_flag_repeat_offender_without_outcome(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            registry.record_launch(
                chain="solana", address="Bad111", token_address="Mint1",
                hard_stops=["HONEYPOT"],
            )
            record = registry.track_record(chain="solana", address="Bad111")
        self.assertTrue(record["is_repeat_offender"])
        self.assertEqual(record["prior_hard_stop_count"], 1)

    def test_reanalyzing_same_token_does_not_inflate_launch_count(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            registry.record_launch(chain="base", address="0xDEP", token_address="0xTOK", score=50)
            registry.record_launch(chain="base", address="0xDEP", token_address="0xTOK", score=60)
            record = registry.track_record(chain="base", address="0xDEP")
        self.assertEqual(record["prior_launch_count"], 1)
        self.assertEqual(record["prior_tokens"][0]["token_address"], "0xTOK")

    def test_outcome_for_unrecorded_launch_is_a_silent_noop(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            # No record_launch call first -- must not raise.
            registry.record_outcome(
                chain="base", address="0xNEW", token_address="0xTOK", adverse=True
            )
            record = registry.track_record(chain="base", address="0xNEW")
        self.assertFalse(record["known"])

    def test_registry_persists_to_disk_and_reloads(self):
        with tempfile.TemporaryDirectory() as temp:
            registry_a = entities.EntityRegistry(temp)
            registry_a.record_launch(chain="base", address="0xDEP", token_address="0xTOK")
            registry_b = entities.EntityRegistry(temp)  # fresh instance, same root
            record = registry_b.track_record(chain="base", address="0xDEP")
        self.assertTrue(record["known"])
        self.assertEqual(record["prior_launch_count"], 1)

    def test_check_and_record_returns_prior_state_before_recording_current(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            first = entities.check_and_record(
                registry, chain="base", address="0xDEP", token_address="0xTOK1"
            )
            # First call: nothing was known before this launch.
            self.assertFalse(first["known"])
            second = entities.check_and_record(
                registry, chain="base", address="0xDEP", token_address="0xTOK2"
            )
            # Second call: the first token is now prior history, but the
            # CURRENT token (TOK2) must not count itself as a prior launch.
            self.assertTrue(second["known"])
            self.assertEqual(second["prior_launch_count"], 1)
            self.assertEqual(second["prior_tokens"][0]["token_address"], "0xTOK1")

    def test_check_and_record_fails_open_on_a_broken_registry(self):
        class BrokenRegistry(entities.EntityRegistry):
            def track_record(self, *, chain, address):
                raise RuntimeError("disk full")

        with tempfile.TemporaryDirectory() as temp:
            registry = BrokenRegistry(temp)
            result = entities.check_and_record(
                registry, chain="base", address="0xDEP", token_address="0xTOK"
            )
        self.assertFalse(result["known"])
        self.assertEqual(result["error"], "RuntimeError")

    def test_registry_file_is_plain_readable_json(self):
        with tempfile.TemporaryDirectory() as temp:
            registry = entities.EntityRegistry(temp)
            registry.record_launch(chain="base", address="0xDEP", token_address="0xTOK", symbol="FOO")
            on_disk = json.loads(Path(temp, "deployer_registry.json").read_text(encoding="utf-8"))
        self.assertIn("base:0xdep", on_disk["entities"])


if __name__ == "__main__":
    unittest.main()
