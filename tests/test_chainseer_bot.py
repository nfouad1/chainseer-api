import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

try:
    import telegram  # noqa: F401

    _TELEGRAM_AVAILABLE = True
except ImportError:
    _TELEGRAM_AVAILABLE = False


def _fake_update(chat_id, text):
    """A minimal stand-in for python-telegram-bot's Update, just enough
    surface for the command handlers under test: effective_chat.id (owner
    check), message.text (argument parsing), message.reply_text (assert
    what was sent back)."""
    message = types.SimpleNamespace(text=text, reply_text=AsyncMock())
    update = types.SimpleNamespace(
        effective_chat=types.SimpleNamespace(id=chat_id),
        effective_user=types.SimpleNamespace(id=chat_id),
        message=message,
    )
    return update, message.reply_text


@unittest.skipUnless(
    _TELEGRAM_AVAILABLE,
    "python-telegram-bot is not installed in this environment",
)
class ChainseerBotSolanaRoutingTests(unittest.IsolatedAsyncioTestCase):
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

    def test_is_owner_requires_matching_configured_chat_id(self):
        update = types.SimpleNamespace(effective_chat=types.SimpleNamespace(id=999))
        with patch.object(self.bot, "OWNER_CHAT_ID", ""):
            self.assertFalse(self.bot._is_owner(update))
        with patch.object(self.bot, "OWNER_CHAT_ID", "999"):
            self.assertTrue(self.bot._is_owner(update))
        with patch.object(self.bot, "OWNER_CHAT_ID", "111"):
            self.assertFalse(self.bot._is_owner(update))

    async def test_cmd_ack_rejects_non_owner(self):
        update, reply = _fake_update(1, "/ack no_change looks fine")
        with patch.object(self.bot, "OWNER_CHAT_ID", "999"):
            await self.bot.cmd_ack(update, None)
        reply.assert_awaited_once()
        self.assertIn("restricted", reply.await_args.args[0])

    async def test_cmd_ack_invalid_outcome_shows_usage(self):
        update, reply = _fake_update(999, "/ack maybe unclear")
        with patch.object(self.bot, "OWNER_CHAT_ID", "999"):
            await self.bot.cmd_ack(update, None)
        reply.assert_awaited_once()
        self.assertIn("Usage", reply.await_args.args[0])

    async def test_cmd_ack_acknowledges_pending_checkpoint(self):
        engine = self._build_engine()
        state = engine.reflection_status()
        state["next_analysis_checkpoint"] = 1
        self.chainseer_solana._atomic_json(engine.reflection_state_path, state)
        engine.observation_ledger.append(
            "solana_risk_analysis", {"mint": self.candidate().mint}
        )
        engine._maybe_request_reflection()
        self.assertTrue(engine.reflection_status()["pause_requested"])
        self.bot._solana_engine = engine

        update, reply = _fake_update(999, "/ack no_change reviewed, all clear")
        with patch.object(self.bot, "OWNER_CHAT_ID", "999"):
            await self.bot.cmd_ack(update, None)
        reply.assert_awaited_once()
        self.assertIn("Acknowledged", reply.await_args.args[0])
        self.assertFalse(engine.reflection_status()["pause_requested"])

    async def test_cmd_reflection_reports_armed_state(self):
        engine = self._build_engine()
        self.bot._solana_engine = engine

        update, reply = _fake_update(999, "/reflection")
        with patch.object(self.bot, "OWNER_CHAT_ID", "999"):
            await self.bot.cmd_reflection(update, None)
        reply.assert_awaited_once()
        self.assertIn("No reflection checkpoint pending", reply.await_args.args[0])

    async def test_cmd_reflection_reports_pending_state_with_context(self):
        engine = self._build_engine()
        state = engine.reflection_status()
        state["next_analysis_checkpoint"] = 1
        self.chainseer_solana._atomic_json(engine.reflection_state_path, state)
        engine.observation_ledger.append(
            "solana_risk_analysis", {"mint": self.candidate().mint}
        )
        engine._maybe_request_reflection()
        self.bot._solana_engine = engine

        update, reply = _fake_update(999, "/reflection")
        with patch.object(self.bot, "OWNER_CHAT_ID", "999"):
            await self.bot.cmd_reflection(update, None)
        reply.assert_awaited_once()
        text = reply.await_args.args[0]
        self.assertIn("paused for review", text)
        self.assertIn("/ack", text)

    async def test_cmd_reflection_rejects_non_owner(self):
        update, reply = _fake_update(1, "/reflection")
        with patch.object(self.bot, "OWNER_CHAT_ID", "999"):
            await self.bot.cmd_reflection(update, None)
        reply.assert_awaited_once()
        self.assertIn("restricted", reply.await_args.args[0])


if __name__ == "__main__":
    unittest.main()
