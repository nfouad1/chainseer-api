import json
import tempfile
import unittest
from pathlib import Path

try:
    import telegram  # noqa: F401

    _TELEGRAM_AVAILABLE = True
except ImportError:
    _TELEGRAM_AVAILABLE = False


@unittest.skipUnless(
    _TELEGRAM_AVAILABLE,
    "python-telegram-bot is not installed in this environment",
)
class ChainseerBotSolanaRoutingTests(unittest.TestCase):
    """chainseer_bot.py routes Solana lookups through SolanaRiskAnalyzer
    (via SolanaPrototypeEngine.evaluate_candidate) when a mint's Pump.fun
    launch can be resolved, and only falls back to the general-purpose
    SolanaPublicAnalyzer when it can't -- see the conversation that added
    resolve_candidate() to chainseer_solana.py's PumpFunObserver."""

    @classmethod
    def setUpClass(cls):
        import chainseer_bot
        import chainseer_solana
        from chainseer_solana_public import SolanaPublicAnalyzer
        from tests.test_chainseer_solana import FakeRPC as SolanaFakeRPC
        from tests.test_chainseer_solana import FakeJupiter, candidate
        from tests.test_chainseer_solana_public import FakeRPC as PublicFakeRPC

        cls.bot = chainseer_bot
        cls.chainseer_solana = chainseer_solana
        cls.SolanaPublicAnalyzer = SolanaPublicAnalyzer
        cls.SolanaFakeRPC = SolanaFakeRPC
        cls.PublicFakeRPC = PublicFakeRPC
        cls.FakeJupiter = FakeJupiter
        cls.candidate = staticmethod(candidate)

    def setUp(self):
        self._saved_engine = self.bot._solana_engine
        self._saved_public = self.bot._solana_public_analyzer
        self.bot._solana_engine = None
        self.bot._solana_public_analyzer = None
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)

    def tearDown(self):
        self.bot._solana_engine = self._saved_engine
        self.bot._solana_public_analyzer = self._saved_public

    def test_extract_solana_mint_ignores_plain_text(self):
        self.assertIsNone(self.bot.extract_solana_mint("hello there"))

    def test_extract_solana_mint_ignores_evm_addresses(self):
        self.assertIsNone(
            self.bot.extract_solana_mint(
                "0x407470F85e0b342a52AaE2F191E135cEF2947777"
            )
        )

    def test_extract_solana_mint_finds_valid_mint_in_free_text(self):
        mint = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
        self.assertEqual(
            self.bot.extract_solana_mint(f"is {mint} safe to buy?"), mint
        )

    def _build_engine(self, *, rpc=None):
        root = Path(self._tmpdir.name) / "learning"
        return self.chainseer_solana.SolanaPrototypeEngine(
            root=root,
            rpc=rpc or self.SolanaFakeRPC(),
            jupiter=self.FakeJupiter(),
            record_timechain=False,
        )

    def test_run_solana_analysis_uses_deep_analyzer_for_a_resolved_pumpfun_launch(self):
        item = self.candidate()
        engine = self._build_engine()
        engine.observer.catalog_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "ecosystem": "pump_fun",
                    "tokens": {item.mint: item.to_dict()},
                }
            ),
            encoding="utf-8",
        )
        self.bot._solana_engine = engine

        mode, summary = self.bot._run_solana_analysis(item.mint)

        self.assertEqual(mode, "deep")
        self.assertIn("Pump.fun launch, verified on-chain", summary)
        self.assertIn("deployer/creator deployment-cadence history", summary)

    def test_run_solana_analysis_falls_back_to_public_analyzer_when_unresolved(self):
        # Default FakeRPC has no signature history, so resolve_candidate()
        # can't find a Pump.fun CreateEvent for this mint at all.
        engine = self._build_engine()
        self.bot._solana_engine = engine
        self.bot._solana_public_analyzer = self.SolanaPublicAnalyzer(
            "https://example.invalid", rpc=self.PublicFakeRPC()
        )

        mint = "So11111111111111111111111111111111111111112"
        mode, summary = self.bot._run_solana_analysis(mint)

        self.assertEqual(mode, "public")
        self.assertIn("general SPL mint check", summary)
        self.assertIn("No verified Pump.fun launch provenance", summary)


if __name__ == "__main__":
    unittest.main()
