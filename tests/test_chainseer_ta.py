import types
import unittest

import chainseer_ta


class FakeEngine:
    def __init__(self, candles):
        self._candles = candles

    def _get_candles(self, _asset, _timeframe):
        return self._candles


def _make_candle(index):
    # A strictly increasing close series means every forward return is
    # positive -- every occurrence "wins" for a long setup regardless of
    # sample count, isolating window sample-size from win/loss outcome.
    close = 100.0 + index
    return types.SimpleNamespace(
        close=close,
        volume=1_000.0,
        high=close + 1,
        low=close - 1,
        date=f"day-{index}",
        open_time=index * 86_400_000,  # one day per candle, in ms
        trades=10,
    )


class WalkForwardMinWindowSamplesTests(unittest.TestCase):
    """windows_passing_55pct/windows_total previously counted any window
    with win_rate > 0.55 regardless of how many occurrences it held -- a
    single lucky trade inflated a pattern to "REAL EDGE". This proves a
    thin window is excluded from the verdict while still being reported."""

    SETUP_ID = "__test_thin_window_setup__"

    def setUp(self):
        candles = [_make_candle(i) for i in range(450)]
        self.engine = FakeEngine(candles)
        # A thick cluster (10 occurrences, well within one ~18-day window)
        # plus one isolated occurrence more than 18 days later, forcing it
        # into its own single-sample window.
        thick_cluster = list(range(210, 220))
        thin_occurrence = [250]
        self.trigger_indices = set(thick_cluster + thin_occurrence)
        chainseer_ta.CANDIDATE_SETUPS[self.SETUP_ID] = {
            "fn": lambda _state, i, triggers=self.trigger_indices: (
                i in triggers
            ),
            "direction": "long",
            "desc": "test-only fixed-index trigger",
        }

    def tearDown(self):
        chainseer_ta.CANDIDATE_SETUPS.pop(self.SETUP_ID, None)

    def test_thin_window_excluded_from_verdict_but_still_reported(self):
        result = chainseer_ta.walk_forward_validate(
            self.engine,
            self.SETUP_ID,
            asset="TEST",
            timeframe="1d",
            window_years=0.05,  # ~18 days
            horizon=7,
            min_window_samples=5,
        )
        self.assertNotIn("error", result)
        self.assertEqual(result["total_occurrences"], 11)

        # Both windows actually happened and both "won" (monotonic uptrend),
        # so pre-fix both would have counted toward windows_passing_55pct.
        self.assertEqual(len(result["windows"]), 2)
        samples = sorted(w["samples"] for w in result["windows"])
        self.assertEqual(samples, [1, 10])
        self.assertTrue(all(w["win_rate"] == 1.0 for w in result["windows"]))

        # Only the 10-sample window counts toward the verdict.
        self.assertEqual(result["windows_total"], 1)
        self.assertEqual(result["windows_passing_55pct"], 1)
        self.assertEqual(
            result["windows_excluded_insufficient_samples"], 1
        )
        self.assertEqual(result["min_window_samples"], 5)

    def test_default_min_window_samples_is_five(self):
        result = chainseer_ta.walk_forward_validate(
            self.engine, self.SETUP_ID, asset="TEST", timeframe="1d",
            window_years=0.05, horizon=7,
        )
        self.assertEqual(result["min_window_samples"], 5)
        self.assertEqual(result["windows_total"], 1)


class IndicatorZeroValueTests(unittest.TestCase):
    """TAEngine.analyze()'s indicator payload used `if x else None`, so a
    genuinely computed 0.0 (MACD histogram at a crossover, price sitting
    exactly on a Bollinger band, RSI at 0) was silently reported as
    missing data. Engineering an exact-zero indicator value via floating
    point is impractical, so this asserts the specific buggy pattern is
    gone from the source rather than the runtime output -- a valid
    technique for this class of bug, and it still runs analyze() end to
    end to confirm the guard rewrite didn't break anything."""

    def test_zero_indicator_values_are_not_reported_as_none(self):
        import inspect

        candles = [_make_candle(i) for i in range(210)]
        fake_self = types.SimpleNamespace(
            _get_candles=lambda _asset, _timeframe: candles,
            ledger=types.SimpleNamespace(to_dict=lambda: {}),
        )
        result = chainseer_ta.TAEngine.analyze(fake_self, "BTC", "1d")
        self.assertIsInstance(result, dict)
        self.assertNotIn("error", result)

        body = inspect.getsource(chainseer_ta.TAEngine.analyze)
        for buggy in (
            "if rsi_val else None",
            "if macd_val else None",
            "if signal_val else None",
            "if hist[i] else None",
            "if bb_upper[i] else None",
            "if bb_mid[i] else None",
            "if bb_lower[i] else None",
            "if bb_position else None",
            "if atr_series[i] else None",
            "if sma50[i] else None",
            "if sma200[i] else None",
            "if ema12[i] else None",
            "if ema26[i] else None",
        ):
            self.assertNotIn(buggy, body)


if __name__ == "__main__":
    unittest.main()
