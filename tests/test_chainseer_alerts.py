import unittest
from unittest.mock import patch, MagicMock

import chainseer_alerts


class ChainseerAlertsTests(unittest.TestCase):
    def setUp(self):
        # Each test gets a clean cooldown table so tests cannot leak state
        # into each other (module-level dict, matching the pattern the rest
        # of this project uses for per-process caches).
        chainseer_alerts._last_sent.clear()

    def test_no_webhook_configured_is_a_silent_noop(self):
        with patch.dict("os.environ", {}, clear=True):
            result = chainseer_alerts.send_alert(
                {"summary": "test"}, chain="base", token_address="0xabc"
            )
        self.assertEqual(result, {"sent": False, "reason": "no_webhook_configured"})

    def test_successful_post_reports_sent_true_and_never_leaks_url(self):
        mock_response = MagicMock(status_code=204)
        with patch("chainseer_alerts.requests.post", return_value=mock_response) as post:
            result = chainseer_alerts.send_alert(
                {"summary": "hard stop fired"},
                chain="base",
                token_address="0xabc",
                webhook_url="https://discord.com/api/webhooks/123/verysecrettoken",
            )
        self.assertTrue(result["sent"])
        self.assertEqual(result["host"], "https://discord.com")
        self.assertNotIn("verysecrettoken", str(result))
        post.assert_called_once()

    def test_network_failure_fails_open_and_never_raises(self):
        with patch(
            "chainseer_alerts.requests.post",
            side_effect=chainseer_alerts.requests.exceptions.ConnectionError("boom"),
        ):
            result = chainseer_alerts.send_alert(
                {"summary": "test"},
                chain="solana",
                token_address="mint123",
                webhook_url="https://example.com/hook",
            )
        self.assertFalse(result["sent"])
        self.assertEqual(result["reason"], "request_error")

    def test_cooldown_collapses_repeated_alerts_for_same_token_and_event(self):
        mock_response = MagicMock(status_code=200)
        with patch("chainseer_alerts.requests.post", return_value=mock_response) as post:
            first = chainseer_alerts.send_alert(
                {"summary": "a"}, chain="base", token_address="0xabc",
                event_type="hard_stop", webhook_url="https://example.com/hook",
                cooldown_seconds=900,
            )
            second = chainseer_alerts.send_alert(
                {"summary": "b"}, chain="base", token_address="0xabc",
                event_type="hard_stop", webhook_url="https://example.com/hook",
                cooldown_seconds=900,
            )
        self.assertTrue(first["sent"])
        self.assertFalse(second["sent"])
        self.assertEqual(second["reason"], "cooldown")
        post.assert_called_once()

    def test_cooldown_is_scoped_per_token_and_event_type(self):
        mock_response = MagicMock(status_code=200)
        with patch("chainseer_alerts.requests.post", return_value=mock_response) as post:
            chainseer_alerts.send_alert(
                {"summary": "a"}, chain="base", token_address="0xabc",
                event_type="hard_stop", webhook_url="https://example.com/hook",
            )
            different_token = chainseer_alerts.send_alert(
                {"summary": "b"}, chain="base", token_address="0xdef",
                event_type="hard_stop", webhook_url="https://example.com/hook",
            )
            different_event = chainseer_alerts.send_alert(
                {"summary": "c"}, chain="base", token_address="0xabc",
                event_type="high_conviction", webhook_url="https://example.com/hook",
            )
        self.assertTrue(different_token["sent"])
        self.assertTrue(different_event["sent"])
        self.assertEqual(post.call_count, 3)

    def test_discord_format_wraps_event_as_chat_content(self):
        mock_response = MagicMock(status_code=200)
        with patch("chainseer_alerts.requests.post", return_value=mock_response) as post:
            chainseer_alerts.send_alert(
                {"summary": "Rug detected", "hard_stops": ["HONEYPOT"]},
                chain="base",
                token_address="0xabc",
                webhook_url="https://discord.com/api/webhooks/x/y",
                webhook_format="discord",
            )
        _, kwargs = post.call_args
        self.assertIn("content", kwargs["json"])
        self.assertIn("Rug detected", kwargs["json"]["content"])
        self.assertIn("HONEYPOT", kwargs["json"]["content"])

    def test_generic_format_sends_raw_event_fields(self):
        mock_response = MagicMock(status_code=200)
        with patch("chainseer_alerts.requests.post", return_value=mock_response) as post:
            chainseer_alerts.send_alert(
                {"summary": "Rug detected"},
                chain="base",
                token_address="0xabc",
                webhook_url="https://example.com/hook",
                webhook_format="generic",
            )
        _, kwargs = post.call_args
        self.assertEqual(kwargs["json"]["summary"], "Rug detected")
        self.assertEqual(kwargs["json"]["chain"], "base")

    def test_alert_on_decision_fires_for_hard_stop(self):
        mock_response = MagicMock(status_code=200)
        with patch("chainseer_alerts.requests.post", return_value=mock_response):
            result = chainseer_alerts.alert_on_decision(
                chain="base", token_address="0xabc", symbol="TEST",
                risk_level="unsafe", score=12.0, hard_stops=["HONEYPOT"],
                webhook_url="https://example.com/hook",
            )
        self.assertTrue(result["sent"])

    def test_alert_on_decision_fires_for_high_conviction_score(self):
        mock_response = MagicMock(status_code=200)
        with patch("chainseer_alerts.requests.post", return_value=mock_response):
            result = chainseer_alerts.alert_on_decision(
                chain="base", token_address="0xabc", symbol="TEST",
                risk_level="safe", score=95.0, hard_stops=[],
                high_conviction_threshold=90.0,
                webhook_url="https://example.com/hook",
            )
        self.assertTrue(result["sent"])

    def test_alert_on_decision_is_a_noop_below_threshold_with_no_hard_stop(self):
        with patch("chainseer_alerts.requests.post") as post:
            result = chainseer_alerts.alert_on_decision(
                chain="base", token_address="0xabc", symbol="TEST",
                risk_level="safe", score=50.0, hard_stops=[],
                high_conviction_threshold=90.0,
                webhook_url="https://example.com/hook",
            )
        self.assertFalse(result["sent"])
        self.assertEqual(result["reason"], "no_trigger")
        post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
