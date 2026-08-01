"""
Chainseer TA Module v1.0 - Technical Analysis for major cryptocurrencies

Separate from Chainseer's on-chain security analysis. This module answers a
different question: "is this a good moment to enter/exit?" rather than
"is this asset legitimate?"

Scope (Phase 1):
  - Assets: BTC, ETH
  - Timeframes: daily (swing), 4h (active)
  - Data: Binance public OHLCV (no API key, full history to 2017)
  - Indicators: RSI, MACD, Bollinger Bands, ATR, SMA/EMA, volume profile

Design principles (from M58 Conceptual Model Construction + chronosynaptic
collapse on Underlying-Pattern Extraction + provenance mirroring):
  1. EVERY indicator value cites its OHLCV source via SHA-256 hash
  2. Classical indicators first (transparent, everyone understands them)
  3. Pattern discovery via Timechain modalities comes in Phase 2
  4. Local computation only - no external TA library - so every value
     traces deterministically to the hashed OHLCV input
  5. Caching with hash verification to avoid refetching

Usage:
    from chainseer_ta import TAEngine
    engine = TAEngine()
    engine.fetch_history('BTC', '1d')   # fetch + cache full history
    report = engine.analyze('BTC', '1d') # compute indicators + verdict

Dependencies:
    pip install requests pandas
"""

import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# Force UTF-8 stdout (same fix as chainseer.py - Windows cp1252 corrupts output)
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, Exception):
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════════════════

BINANCE_API = "https://api.binance.com/api/v3/klines"

# Assets we support (Phase 1: BTC + ETH)
SUPPORTED_ASSETS = {
    "BTC": {"symbol": "BTCUSDT", "name": "Bitcoin"},
    "ETH": {"symbol": "ETHUSDT", "name": "Ethereum"},
}

# Timeframes: Binance interval code -> human label
SUPPORTED_TIMEFRAMES = {
    "1d": "Daily",
    "4h": "4-Hour",
}

# Binance returns max 1000 candles per request; paginate for history
BINANCE_PAGE_SIZE = 1000

# Cache directory
CACHE_DIR = Path(__file__).parent / "ta_cache"
CACHE_DIR.mkdir(exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TA Provenance Ledger (mirrors Chainseer core's ProvenanceLedger)
# ═══════════════════════════════════════════════════════════════════════════════

class TAProvenanceLedger:
    """Records every OHLCV fetch with query spec + response hash.

    Every indicator value traces back through this ledger to the exact
    Binance API call and the SHA-256 of the candles it was computed from.
    This is the TA equivalent of Chainseer core's on-chain provenance.
    """

    def __init__(self):
        self._facts = []
        self._snapshot_time = None

    @property
    def snapshot_time(self):
        return self._snapshot_time

    @snapshot_time.setter
    def snapshot_time(self, value):
        if self._snapshot_time is None:
            self._snapshot_time = value

    def record(self, source: str, query: dict, response_raw) -> str:
        """Record a fetch; return fact_id for citation."""
        resp_hash = hashlib.sha256(
            json.dumps(response_raw, sort_keys=True, default=str).encode()
        ).hexdigest()
        fact_id = f"TA{len(self._facts):04d}"
        self._facts.append({
            "fact_id": fact_id,
            "source": source,
            "query": query,
            "response_hash": resp_hash,
            "response_hash_short": resp_hash[:16],
            "candle_count": len(response_raw) if isinstance(response_raw, list) else None,
            "snapshot_time": self._snapshot_time,
        })
        return fact_id

    def to_dict(self) -> dict:
        return {
            "snapshot_time": self._snapshot_time,
            "fact_count": len(self._facts),
            "facts": list(self._facts),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# OHLCV Data Layer
# ═══════════════════════════════════════════════════════════════════════════════

class OHLCVData:
    """A single OHLCV candle."""

    __slots__ = ("open_time", "open", "high", "low", "close", "volume",
                 "close_time", "quote_volume", "trades")

    def __init__(self, row: list):
        # Binance format: [openTime, o, h, l, c, v, closeTime, quoteVol, trades, ...]
        self.open_time = row[0]
        self.open = float(row[1])
        self.high = float(row[2])
        self.low = float(row[3])
        self.close = float(row[4])
        self.volume = float(row[5])
        self.close_time = row[6]
        self.quote_volume = float(row[7])
        self.trades = int(row[8])

    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.open_time / 1000, tz=timezone.utc).date().isoformat()

    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "quote_volume": self.quote_volume,
            "trades": self.trades,
        }


def _fetch_klines_page(symbol: str, interval: str, start_ms: int, end_ms: int,
                       ledger: TAProvenanceLedger) -> list:
    """Fetch one page (up to 1000 candles) from Binance."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": BINANCE_PAGE_SIZE,
    }
    resp = requests.get(BINANCE_API, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # Record provenance BEFORE returning - the raw response is the evidence
    ledger.record(
        source="binance",
        query={"url": BINANCE_API, "params": dict(params)},
        response_raw=data,
    )
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# Classical Indicators (locally computed - no external TA lib)
# ═══════════════════════════════════════════════════════════════════════════════

def sma(values: list, period: int) -> list:
    """Simple Moving Average. Returns list same length (None for warmup)."""
    out = [None] * len(values)
    if len(values) < period:
        return out
    running = sum(values[:period])
    out[period - 1] = running / period
    for i in range(period, len(values)):
        running += values[i] - values[i - period]
        out[i] = running / period
    return out


def ema(values: list, period: int) -> list:
    """Exponential Moving Average."""
    out = [None] * len(values)
    if len(values) < period:
        return out
    multiplier = 2 / (period + 1)
    # Seed with SMA
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    for i in range(period, len(values)):
        out[i] = values[i] * multiplier + out[i - 1] * (1 - multiplier)
    return out


def rsi(closes: list, period: int = 14) -> list:
    """Relative Strength Index (Wilder's smoothing)."""
    out = [None] * len(closes)
    if len(closes) < period + 1:
        return out
    gains, losses = [], []
    for i in range(1, period + 1):
        change = closes[i] - closes[i - 1]
        gains.append(max(0, change))
        losses.append(max(0, -change))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        out[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        out[period] = 100 - (100 / (1 + rs))
    for i in range(period + 1, len(closes)):
        change = closes[i] - closes[i - 1]
        gain = max(0, change)
        loss = max(0, -change)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0:
            out[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100 - (100 / (1 + rs))
    return out


def macd(closes: list, fast: int = 12, slow: int = 26, signal: int = 9):
    """MACD line, signal line, histogram."""
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = []
    for f, s in zip(ema_fast, ema_slow):
        macd_line.append(f - s if (f is not None and s is not None) else None)
    # Signal = EMA of MACD line (over non-None portion)
    first_valid = next((i for i, v in enumerate(macd_line) if v is not None), None)
    signal_line = [None] * len(closes)
    if first_valid is not None:
        valid = macd_line[first_valid:]
        sig = ema(valid, signal)
        for i, v in enumerate(sig):
            signal_line[first_valid + i] = v
    histogram = []
    for m, s in zip(macd_line, signal_line):
        histogram.append(m - s if (m is not None and s is not None) else None)
    return macd_line, signal_line, histogram


def bollinger_bands(closes: list, period: int = 20, std_dev: float = 2.0):
    """Bollinger Bands: middle (SMA), upper, lower."""
    mid = sma(closes, period)
    upper, lower = [None] * len(closes), [None] * len(closes)
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1: i + 1]
        mean = mid[i]
        variance = sum((x - mean) ** 2 for x in window) / period
        sd = variance ** 0.5
        upper[i] = mean + std_dev * sd
        lower[i] = mean - std_dev * sd
    return mid, upper, lower


def atr(highs: list, lows: list, closes: list, period: int = 14) -> list:
    """Average True Range."""
    out = [None] * len(closes)
    if len(closes) < period + 1:
        return out
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    # Wilder smoothing
    atr_val = sum(trs[:period]) / period
    out[period] = atr_val
    for i in range(period, len(trs)):
        atr_val = (atr_val * (period - 1) + trs[i]) / period
        out[i + 1] = atr_val
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# TA Engine
# ═══════════════════════════════════════════════════════════════════════════════

class TAEngine:
    """Fetches OHLCV, caches locally, computes indicators with provenance."""

    def __init__(self):
        self.ledger = TAProvenanceLedger()
        self.ledger.snapshot_time = datetime.now(timezone.utc).isoformat()
        self._cache = {}  # (asset, timeframe) -> list[OHLCVData]

    def fetch_history(self, asset: str, timeframe: str, verbose: bool = True):
        """Fetch full OHLCV history from Binance, cache locally.

        Paginates through Binance's 1000-candle limit. Caches to disk so
        subsequent runs only fetch new candles. Each page's raw response
        is hashed into the provenance ledger.
        """
        if asset not in SUPPORTED_ASSETS:
            raise ValueError(f"Unsupported asset: {asset}. Use one of {list(SUPPORTED_ASSETS)}")
        if timeframe not in SUPPORTED_TIMEFRAMES:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        symbol = SUPPORTED_ASSETS[asset]["symbol"]
        cache_file = CACHE_DIR / f"{asset}_{timeframe}.json"

        # Load existing cache to find resume point
        existing = []
        last_open_time = 0
        if cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cached_raw = f.read()
                cached = json.loads(cached_raw)
                existing = cached.get("candles", [])
                if existing:
                    last_open_time = existing[-1]["open_time"]
                    # Record provenance for the cache load — the cached file
                    # is itself verifiable evidence (its hash ties the indicator
                    # values to a specific snapshot of OHLCV data)
                    self.ledger.record(
                        source="cache",
                        query={"file": str(cache_file), "asset": asset, "timeframe": timeframe},
                        response_raw=cached,
                    )
                if verbose:
                    print(f"  Cache: {len(existing)} candles loaded (last: {existing[-1]['date']})")
            except Exception:
                pass

        # Determine fetch window
        if last_open_time > 0:
            start_ms = last_open_time + 1  # resume after last cached
        else:
            start_ms = 0  # full history from earliest

        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Paginate
        new_candles = []
        page_count = 0
        while start_ms < end_ms:
            page = _fetch_klines_page(symbol, timeframe, start_ms, end_ms, self.ledger)
            page_count += 1
            if not page:
                break
            for row in page:
                c = OHLCVData(row)
                new_candles.append({
                    "open_time": c.open_time,
                    "date": c.date,
                    "open": c.open,
                    "high": c.high,
                    "low": c.low,
                    "close": c.close,
                    "volume": c.volume,
                    "quote_volume": c.quote_volume,
                    "trades": c.trades,
                    "close_time": c.close_time,
                })
            # Advance past the last candle in this page
            start_ms = page[-1][0] + 1
            if len(page) < BINANCE_PAGE_SIZE:
                break
            time.sleep(0.15)  # respect rate limit

        if verbose:
            print(f"  Fetched {len(new_candles)} new candles ({page_count} page(s))")

        # Merge: existing + new (dedup by open_time)
        by_time = {c["open_time"]: c for c in existing}
        for c in new_candles:
            by_time[c["open_time"]] = c
        merged = sorted(by_time.values(), key=lambda c: c["open_time"])

        # Cache to disk
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump({"asset": asset, "timeframe": timeframe, "candles": merged}, f)

        # In-memory cache as OHLCVData objects
        self._cache[(asset, timeframe)] = [OHLCVData([
            c["open_time"], c["open"], c["high"], c["low"], c["close"],
            c["volume"], c.get("close_time", c["open_time"]),
            c["quote_volume"], c["trades"], 0, 0, 0,
        ]) for c in merged]

        if verbose:
            print(f"  Total: {len(merged)} candles cached")
        return len(merged)

    def _get_candles(self, asset: str, timeframe: str) -> list:
        key = (asset, timeframe)
        if key not in self._cache:
            self.fetch_history(asset, timeframe, verbose=False)
        return self._cache[key]

    def _get_closes_or_fetch(self, asset: str, timeframe: str) -> list:
        """Fetch candles and return them (alias for lead-lag analysis)."""
        return self._get_candles(asset, timeframe)

    def analyze(self, asset: str, timeframe: str, lookback: int = 200) -> dict:
        """Compute all indicators for the most recent `lookback` candles.

        Returns a dict with current indicator values + provenance summary.
        Every value traces to the hashed OHLCV via the ledger.
        """
        candles = self._get_candles(asset, timeframe)
        if len(candles) < 50:
            return {"error": f"Insufficient data: {len(candles)} candles"}

        # Compute over full series (need warmup), report last `lookback`
        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]
        volumes = [c.volume for c in candles]

        rsi_series = rsi(closes, 14)
        macd_line, signal_line, hist = macd(closes)
        bb_mid, bb_upper, bb_lower = bollinger_bands(closes, 20, 2.0)
        atr_series = atr(highs, lows, closes, 14)
        sma50 = sma(closes, 50)
        sma200 = sma(closes, 200)
        ema12 = ema(closes, 12)
        ema26 = ema(closes, 26)

        # Current values (last candle)
        i = len(closes) - 1
        last = candles[i]

        # Distance from Bollinger Bands (% of width)
        if bb_upper[i] and bb_lower[i] and bb_mid[i]:
            bb_width = bb_upper[i] - bb_lower[i]
            bb_position = (last.close - bb_lower[i]) / bb_width * 100 if bb_width > 0 else 50
        else:
            bb_position = None

        # Trend: price vs SMA50 vs SMA200
        trend = "unknown"
        if sma50[i] and sma200[i]:
            if last.close > sma50[i] > sma200[i]:
                trend = "bullish"
            elif last.close < sma50[i] < sma200[i]:
                trend = "bearish"
            else:
                trend = "mixed"

        # RSI interpretation
        rsi_val = rsi_series[i]
        if rsi_val is not None:
            if rsi_val >= 70:
                rsi_label = "overbought"
            elif rsi_val <= 30:
                rsi_label = "oversold"
            else:
                rsi_label = "neutral"
        else:
            rsi_label = "warming up"

        # MACD interpretation
        macd_val = macd_line[i]
        signal_val = signal_line[i]
        if macd_val is not None and signal_val is not None:
            macd_label = "bullish" if macd_val > signal_val else "bearish"
        else:
            macd_label = "warming up"

        # Volume context: last candle vs 20-period average
        vol_now = volumes[i]
        vol_avg_20 = sum(volumes[max(0, i - 19):i + 1]) / min(20, i + 1)
        vol_ratio = vol_now / vol_avg_20 if vol_avg_20 > 0 else 1.0

        return {
            "asset": asset,
            "name": SUPPORTED_ASSETS[asset]["name"],
            "timeframe": timeframe,
            "timeframe_label": SUPPORTED_TIMEFRAMES[timeframe],
            "latest_candle": {
                "date": last.date,
                "close": last.close,
                "volume": last.volume,
                "trades": last.trades,
            },
            # `is not None` (not truthiness) below: these series use None
            # only for warmup indices with insufficient history (see sma()/
            # ema()); a genuinely computed 0.0 -- MACD histogram at a
            # crossover, price sitting exactly on a Bollinger band -- is a
            # real, meaningful value, not missing data, and must not be
            # reported as None.
            "indicators": {
                "rsi_14": {"value": round(rsi_val, 2) if rsi_val is not None else None, "label": rsi_label},
                "macd": {
                    "value": round(macd_val, 2) if macd_val is not None else None,
                    "signal": round(signal_val, 2) if signal_val is not None else None,
                    "histogram": round(hist[i], 2) if hist[i] is not None else None,
                    "label": macd_label,
                },
                "bollinger": {
                    "upper": round(bb_upper[i], 2) if bb_upper[i] is not None else None,
                    "middle": round(bb_mid[i], 2) if bb_mid[i] is not None else None,
                    "lower": round(bb_lower[i], 2) if bb_lower[i] is not None else None,
                    "position_pct": round(bb_position, 1) if bb_position is not None else None,
                },
                "atr_14": {"value": round(atr_series[i], 2) if atr_series[i] is not None else None},
                "sma_50": {"value": round(sma50[i], 2) if sma50[i] is not None else None},
                "sma_200": {"value": round(sma200[i], 2) if sma200[i] is not None else None},
                "ema_12": {"value": round(ema12[i], 2) if ema12[i] is not None else None},
                "ema_26": {"value": round(ema26[i], 2) if ema26[i] is not None else None},
            },
            "interpretation": {
                "trend": trend,
                "rsi_label": rsi_label,
                "macd_label": macd_label,
                "volume_ratio": round(vol_ratio, 2),
                "volume_label": "above average" if vol_ratio > 1.5 else (
                    "below average" if vol_ratio < 0.7 else "average"),
            },
            "data_span": {
                "first_date": candles[0].date,
                "last_date": candles[-1].date,
                "candle_count": len(candles),
            },
            "provenance": self.ledger.to_dict(),
        }

    def print_report(self, analysis: dict):
        """Print a concise TA report (mirrors Chainseer summary philosophy)."""
        if "error" in analysis:
            print(f" ERROR: {analysis['error']}")
            return

        a = analysis
        ind = a["indicators"]
        interp = a["interpretation"]
        lc = a["latest_candle"]

        icons = {
            "bullish": "[BULLISH]", "bearish": "[BEARISH]",
            "mixed": "[MIXED]", "overbought": "[OVERBOUGHT]",
            "oversold": "[OVERSOLD]", "neutral": "[NEUTRAL]",
            "unknown": "[?]", "warming up": "[warmup]",
            "above average": "[HIGH VOL]", "below average": "[LOW VOL]",
            "average": "[AVG VOL]",
        }
        icon = lambda x: icons.get(x, "")

        print("-" * 56)
        print(f" CHAINSEER TA  {a['name']} ({a['asset']}) - {a['timeframe_label']}")
        print(f" {lc['date']}  Close: ${lc['close']:,.2f}  Vol: {lc['volume']:,.0f}")
        print("-" * 56)
        print(f" Trend:     {interp['trend'].upper()} {icon(interp['trend'])}")
        print(f" RSI(14):   {ind['rsi_14']['value']} {icon(ind['rsi_14']['label'])}")
        print(f" MACD:      {ind['macd']['label']} {icon(ind['macd']['label'])}"
              f"  (hist: {ind['macd']['histogram']})")
        print(f" BB pos:    {ind['bollinger']['position_pct']}% of band width")
        print(f" ATR(14):   ${ind['atr_14']['value']}")
        print(f" SMA50/200: ${ind['sma_50']['value']} / ${ind['sma_200']['value']}")
        print(f" Volume:    {interp['volume_label']} {icon(interp['volume_label'])}"
              f" ({interp['volume_ratio']}x avg)")
        print("-" * 56)
        print(f" Data: {a['data_span']['candle_count']} candles "
              f"({a['data_span']['first_date']} to {a['data_span']['last_date']})")
        prov = a.get("provenance", {})
        print(f" Provenance: {prov.get('fact_count', 0)} facts "
              f"(each indicator traces to hashed OHLCV)")
        print("-" * 56)


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 2: Pattern Discovery & Validation
# Engages M51 (Underlying-Pattern Extraction), M76 (Recurring-Pattern Recognition),
# M92 (Falsification-Discovered Reasoning), S102 (Emerging-Pattern Foresight)
#
# Methodology (honest, anti-p-hacking):
#   1. Define candidate setups A PRIORI (before looking at outcomes)
#   2. For each setup occurrence, measure FORWARD returns (not backward fit)
#   3. Classify each occurrence by market regime (bull/bear/chop)
#   4. Win rate = % occurrences where forward return > 0
#   5. Statistical filter: keep only setups with >=30 samples AND >55% win rate
#      AND positive expectancy (avg win > avg loss)
#   6. Seal surviving patterns to Timechain for future falsification
# ═══════════════════════════════════════════════════════════════════════════════

# Forward-return horizons to measure (in candles)
FORWARD_HORIZONS = [1, 3, 7, 14, 30]


def _forward_returns(closes: list, i: int) -> dict:
    """Compute forward returns at each horizon from candle index i.

    Returns {1: 0.02, 3: 0.05, ...} or {} if not enough future data.
    """
    out = {}
    for h in FORWARD_HORIZONS:
        if i + h < len(closes):
            out[h] = (closes[i + h] - closes[i]) / closes[i]
    return out


def classify_regime(closes: list, sma200: list, i: int) -> str:
    """Classify market regime at index i using SMA200 slope.

    bull:  price above rising SMA200
    bear:  price below falling SMA200
    chop:  everything else (consolidation, transitions)
    """
    if sma200[i] is None or i < 210:
        return "unknown"
    # Slope over last 20 SMA200 values
    lookback = 20
    slope_window = [sma200[j] for j in range(i - lookback, i + 1) if sma200[j] is not None]
    if len(slope_window) < 10:
        return "unknown"
    slope = (slope_window[-1] - slope_window[0]) / slope_window[0]
    if closes[i] > sma200[i] and slope > 0.01:
        return "bull"
    elif closes[i] < sma200[i] and slope < -0.01:
        return "bear"
    else:
        return "chop"


class PatternOccurrence:
    """A single occurrence of a setup at a specific candle."""

    __slots__ = ("setup_id", "date", "index", "price", "regime",
                 "forward_returns")

    def __init__(self, setup_id, date, index, price, regime, forward_returns):
        self.setup_id = setup_id
        self.date = date
        self.index = index
        self.price = price
        self.regime = regime
        self.forward_returns = forward_returns

    def to_dict(self):
        return {
            "date": self.date,
            "price": self.price,
            "regime": self.regime,
            "forward_returns": {k: round(v, 4) for k, v in self.forward_returns.items()},
        }


# ── A PRIORI candidate setups (defined before looking at outcomes) ───────────
# Each setup function takes indicator series + candle index, returns True/False.

def setup_rsi_oversold(state: dict, i: int) -> bool:
    """RSI(14) crosses below 30 (classic oversold)."""
    rsi_series = state["rsi_14"]
    if i < 1 or rsi_series[i] is None or rsi_series[i - 1] is None:
        return False
    return rsi_series[i - 1] >= 30 > rsi_series[i]


def setup_rsi_overbought(state: dict, i: int) -> bool:
    """RSI(14) crosses above 70 (classic overbought)."""
    rsi_series = state["rsi_14"]
    if i < 1 or rsi_series[i] is None or rsi_series[i - 1] is None:
        return False
    return rsi_series[i - 1] <= 70 < rsi_series[i]


def setup_macd_bull_cross(state: dict, i: int) -> bool:
    """MACD line crosses above signal line."""
    macd_line = state["macd_line"]
    signal_line = state["signal_line"]
    if i < 1 or macd_line[i] is None or signal_line[i] is None:
        return False
    return macd_line[i - 1] <= signal_line[i - 1] and macd_line[i] > signal_line[i]


def setup_macd_bear_cross(state: dict, i: int) -> bool:
    """MACD line crosses below signal line."""
    macd_line = state["macd_line"]
    signal_line = state["signal_line"]
    if i < 1 or macd_line[i] is None or signal_line[i] is None:
        return False
    return macd_line[i - 1] >= signal_line[i - 1] and macd_line[i] < signal_line[i]


def setup_bb_lower_touch(state: dict, i: int) -> bool:
    """Close touches or breaches lower Bollinger Band."""
    closes = state["closes"]
    bb_lower = state["bb_lower"]
    if bb_lower[i] is None:
        return False
    return closes[i] <= bb_lower[i]


def setup_bb_upper_touch(state: dict, i: int) -> bool:
    """Close touches or breaches upper Bollinger Band."""
    closes = state["closes"]
    bb_upper = state["bb_upper"]
    if bb_upper[i] is None:
        return False
    return closes[i] >= bb_upper[i]


def setup_golden_cross(state: dict, i: int) -> bool:
    """SMA50 crosses above SMA200 (long-term bullish)."""
    sma50 = state["sma_50"]
    sma200 = state["sma_200"]
    if i < 1 or sma50[i] is None or sma200[i] is None:
        return False
    return sma50[i - 1] <= sma200[i - 1] and sma50[i] > sma200[i]


def setup_death_cross(state: dict, i: int) -> bool:
    """SMA50 crosses below SMA200 (long-term bearish)."""
    sma50 = state["sma_50"]
    sma200 = state["sma_200"]
    if i < 1 or sma50[i] is None or sma200[i] is None:
        return False
    return sma50[i - 1] >= sma200[i - 1] and sma50[i] < sma200[i]


def setup_volume_spike(state: dict, i: int) -> bool:
    """Volume > 2x the 20-period average."""
    volumes = state["volumes"]
    if i < 20:
        return False
    avg = sum(volumes[i - 20:i]) / 20
    return avg > 0 and volumes[i] > 2 * avg


# Registry of all candidate setups
CANDIDATE_SETUPS = {
    "rsi_oversold": {"fn": setup_rsi_oversold, "direction": "long",
                     "desc": "RSI(14) crosses below 30"},
    "rsi_overbought": {"fn": setup_rsi_overbought, "direction": "short",
                       "desc": "RSI(14) crosses above 70"},
    "macd_bull_cross": {"fn": setup_macd_bull_cross, "direction": "long",
                        "desc": "MACD line crosses above signal"},
    "macd_bear_cross": {"fn": setup_macd_bear_cross, "direction": "short",
                        "desc": "MACD line crosses below signal"},
    "bb_lower_touch": {"fn": setup_bb_lower_touch, "direction": "long",
                       "desc": "Close <= lower Bollinger Band"},
    "bb_upper_touch": {"fn": setup_bb_upper_touch, "direction": "short",
                       "desc": "Close >= upper Bollinger Band"},
    "golden_cross": {"fn": setup_golden_cross, "direction": "long",
                     "desc": "SMA50 crosses above SMA200"},
    "death_cross": {"fn": setup_death_cross, "direction": "short",
                    "desc": "SMA50 crosses below SMA200"},
    "volume_spike": {"fn": setup_volume_spike, "direction": "long",
                     "desc": "Volume > 2x 20-period average"},
}


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 3: Confluence Patterns + Regime-Aware Discovery
# Engages M94 (Confluence-Compression Reasoning), S102 (Emerging-Pattern Foresight)
#
# Phase 2 proved singles are noise. Phase 3 tests ECONOMICALLY MOTIVATED
# combinations only - not brute-force 2^9 (that would be p-hacking).
# Each confluence pattern has an economic thesis behind it:
#   capitulation: panic exhaustion (3 confirmations of oversold + volume)
#   momentum_ignition: trend birth with conviction
#   bb_squeeze_breakout: volatility compression precedes expansion
#   regime-filtered volume_spike: edge only in chop (per Phase 2 finding)
# ═══════════════════════════════════════════════════════════════════════════════

def setup_capitulation(state: dict, i: int) -> bool:
    """Capitulation: RSI<35 AND BB lower touch AND volume spike.

    Economic thesis: panic selling exhausts itself when 3 independent
    signals confirm oversold + unusual volume. This is the "blood in the
    streets" contrarian entry.
    """
    return (setup_rsi_oversold(state, i) or _rsi_below(state, i, 35)) \
        and setup_bb_lower_touch(state, i) \
        and setup_volume_spike(state, i)


def _rsi_below(state: dict, i: int, threshold: float) -> bool:
    """Helper: RSI below threshold at index i."""
    rsi_series = state["rsi_14"]
    return rsi_series[i] is not None and rsi_series[i] < threshold


def _rsi_above(state: dict, i: int, threshold: float) -> bool:
    """Helper: RSI above threshold at index i."""
    rsi_series = state["rsi_14"]
    return rsi_series[i] is not None and rsi_series[i] > threshold


def setup_momentum_ignition(state: dict, i: int) -> bool:
    """Momentum ignition: MACD bull cross AND price>SMA50 AND volume spike.

    Economic thesis: a MACD cross alone is noise (Phase 2 proved 50% win
    rate), but a MACD cross WITH price already above the medium-term trend
    AND unusual volume suggests trend ignition with conviction.
    """
    closes = state["closes"]
    sma50 = state["sma_50"]
    return setup_macd_bull_cross(state, i) \
        and sma50[i] is not None and closes[i] > sma50[i] \
        and setup_volume_spike(state, i)


def setup_bb_squeeze(state: dict, i: int) -> bool:
    """Bollinger Band squeeze: BB width at 90-day low.

    Economic thesis: volatility is mean-reverting. Extreme compression
    (the "squeeze") tends to precede expansion. The signal fires when BB
    width reaches its 90-day minimum - a coiled spring.
    """
    bb_upper = state["bb_upper"]
    bb_lower = state["bb_lower"]
    if bb_upper[i] is None or bb_lower[i] is None or i < 90:
        return False
    width_now = (bb_upper[i] - bb_lower[i])
    if width_now <= 0:
        return False
    # Find minimum width over prior 90 days
    widths = []
    for j in range(i - 89, i + 1):
        if bb_upper[j] is not None and bb_lower[j] is not None:
            widths.append(bb_upper[j] - bb_lower[j])
    if not widths or len(widths) < 60:
        return False
    min_width = min(widths)
    # Fire when current width is at or near the 90-day minimum (within 5%)
    return width_now <= min_width * 1.05


def setup_bb_squeeze_breakup(state: dict, i: int) -> bool:
    """BB squeeze followed by upward breakout.

    Economic thesis: the squeeze itself is the setup; the breakout
    direction (up) is the trigger. Fires when BB was squeezed within
    last 3 candles AND current close > BB upper (expansion upward).
    """
    bb_upper = state["bb_upper"]
    closes = state["closes"]
    if i < 93 or bb_upper[i] is None:
        return False
    # Was squeezed in last 3 candles?
    was_squeezed = any(setup_bb_squeeze(state, j) for j in range(max(0, i - 3), i))
    if not was_squeezed:
        return False
    # Breaking upward now
    return closes[i] > bb_upper[i]


def setup_regime_chop_volume(state: dict, i: int) -> bool:
    """Volume spike IN CHOP REGIME ONLY (Phase 2 informed).

    Phase 2 finding: volume_spike had 91.5% win rate in chop but 31.8%
    in bear. This setup fires ONLY when regime is chop, gating the
    signal to where the edge actually exists. This is regime-aware
    signal refinement, not a new pattern.
    """
    if not setup_volume_spike(state, i):
        return False
    closes = state["closes"]
    sma200 = state["sma_200"]
    regime = classify_regime(closes, sma200, i)
    return regime == "chop"


# Registry of confluence patterns (Phase 3) - economically motivated only
CONFLUENCE_SETUPS = {
    "capitulation": {"fn": setup_capitulation, "direction": "long",
                     "desc": "RSI<35 + BB lower + volume spike (panic exhaustion)"},
    "momentum_ignition": {"fn": setup_momentum_ignition, "direction": "long",
                          "desc": "MACD bull cross + price>SMA50 + volume spike"},
    "bb_squeeze": {"fn": setup_bb_squeeze, "direction": "long",
                   "desc": "BB width at 90-day low (coiled spring)"},
    "bb_squeeze_breakup": {"fn": setup_bb_squeeze_breakup, "direction": "long",
                           "desc": "Squeeze then upward breakout"},
    "regime_chop_volume": {"fn": setup_regime_chop_volume, "direction": "long",
                           "desc": "Volume spike in chop regime only (Phase 2 informed)"},
}


def discover_confluence(engine: "TAEngine", asset: str, timeframe: str) -> dict:
    """Phase 3: discover confluence + regime-aware patterns.

    Uses the same forward-return measurement as Phase 2's discover_patterns,
    but applies it to the CONFLUENCE_SETUPS registry instead.
    """
    candles = engine._get_candles(asset, timeframe)
    if len(candles) < 250:
        return {"error": f"Insufficient data: {len(candles)} candles (need 250+)"}

    closes = [c.close for c in candles]
    highs = [c.high for c in candles]
    lows = [c.low for c in candles]
    volumes = [c.volume for c in candles]

    rsi_series = rsi(closes, 14)
    macd_line, signal_line, _ = macd(closes)
    _, bb_upper, bb_lower = bollinger_bands(closes, 20, 2.0)
    sma50 = sma(closes, 50)
    sma200 = sma(closes, 200)

    state = {
        "closes": closes, "highs": highs, "lows": lows, "volumes": volumes,
        "rsi_14": rsi_series, "macd_line": macd_line, "signal_line": signal_line,
        "bb_upper": bb_upper, "bb_lower": bb_lower, "sma_50": sma50, "sma_200": sma200,
    }

    results = {}
    for setup_id, setup_def in CONFLUENCE_SETUPS.items():
        occurrences = []
        fn = setup_def["fn"]
        for i in range(210, len(closes) - max(FORWARD_HORIZONS)):
            if fn(state, i):
                regime = classify_regime(closes, sma200, i)
                fwd = _forward_returns(closes, i)
                if fwd:
                    occurrences.append(PatternOccurrence(
                        setup_id=setup_id, date=candles[i].date, index=i,
                        price=closes[i], regime=regime, forward_returns=fwd,
                    ))
        results[setup_id] = occurrences

    return {
        "asset": asset,
        "timeframe": timeframe,
        "total_candles": len(candles),
        "analysis_window": f"{candles[210].date} to {candles[-1].date}",
        "results": results,
        "candidate_count": len(CONFLUENCE_SETUPS),
    }


def analyze_lead_lag(engine: "TAEngine", timeframe: str,
                     threshold_pct: float = 3.0, max_lag: int = 12) -> dict:
    """Cross-asset lead-lag: does BTC lead ETH?

    For each candle where BTC moved > threshold_pct, check ETH's return
    over the next 1..max_lag candles. If ETH consistently follows,
    there's a lead-lag relationship exploitable for ETH entry timing.

    This is a pattern that ISN'T in textbooks and ISN'T a single-indicator
    signal - it's a structural cross-asset relationship.
    """
    btc_candles = engine._get_candles("BTC", timeframe)
    eth_candles = engine._get_closes_or_fetch("ETH", timeframe)

    # Align by open_time
    btc_by_time = {c.open_time: c for c in btc_candles}
    eth_by_time = {c.open_time: c for c in eth_candles}
    common_times = sorted(set(btc_by_time) & set(eth_by_time))

    if len(common_times) < 300:
        return {"error": f"Insufficient overlapping data: {len(common_times)}"}

    btc_closes = [btc_by_time[t].close for t in common_times]
    eth_closes = [eth_by_time[t].close for t in common_times]

    # Find BTC moves > threshold
    btc_moves = []
    for i in range(1, len(btc_closes) - max_lag):
        ret = (btc_closes[i] - btc_closes[i - 1]) / btc_closes[i - 1]
        if abs(ret) >= threshold_pct / 100:
            # Measure ETH returns over next 1..max_lag candles
            eth_forward = {}
            for lag in range(1, max_lag + 1):
                if i + lag < len(eth_closes):
                    eth_forward[lag] = (eth_closes[i + lag] - eth_closes[i]) / eth_closes[i]
            if eth_forward:
                btc_moves.append({
                    "btc_return": ret,
                    "direction": "up" if ret > 0 else "down",
                    "eth_forward": eth_forward,
                })

    if not btc_moves:
        return {"error": f"No BTC moves >= {threshold_pct}% found"}

    # Aggregate: for BTC up moves, does ETH follow up? For BTC down moves, ETH down?
    stats = {"up": {"count": 0, "eth_follow_rate": {}},
             "down": {"count": 0, "eth_follow_rate": {}}}
    for direction in ("up", "down"):
        dir_moves = [m for m in btc_moves if m["direction"] == direction]
        stats[direction]["count"] = len(dir_moves)
        for lag in range(1, max_lag + 1):
            follows = 0
            total = 0
            for m in dir_moves:
                if lag in m["eth_forward"]:
                    total += 1
                    if (direction == "up" and m["eth_forward"][lag] > 0) or \
                       (direction == "down" and m["eth_forward"][lag] < 0):
                        follows += 1
            stats[direction]["eth_follow_rate"][lag] = {
                "follow_rate": round(follows / total, 3) if total > 0 else None,
                "samples": total,
            }

    return {
        "timeframe": timeframe,
        "threshold_pct": threshold_pct,
        "max_lag_candles": max_lag,
        "total_btc_moves": len(btc_moves),
        "alignment_window": f"{datetime.fromtimestamp(common_times[0]/1000, tz=timezone.utc).date()} to {datetime.fromtimestamp(common_times[-1]/1000, tz=timezone.utc).date()}",
        "stats": stats,
    }



def discover_patterns(engine: "TAEngine", asset: str, timeframe: str) -> dict:
    """Run pattern discovery: find all setup occurrences, measure forward returns.

    This is the M51+M76 core: scan history for each candidate setup, record
    each occurrence's forward returns, then aggregate win rates per regime.
    """
    candles = engine._get_candles(asset, timeframe)
    if len(candles) < 250:
        return {"error": f"Insufficient data: {len(candles)} candles (need 250+)"}

    closes = [c.close for c in candles]
    highs = [c.high for c in candles]
    lows = [c.low for c in candles]
    volumes = [c.volume for c in candles]

    # Compute all indicator series once
    rsi_series = rsi(closes, 14)
    macd_line, signal_line, _ = macd(closes)
    _, bb_upper, bb_lower = bollinger_bands(closes, 20, 2.0)
    sma50 = sma(closes, 50)
    sma200 = sma(closes, 200)

    state = {
        "closes": closes, "highs": highs, "lows": lows, "volumes": volumes,
        "rsi_14": rsi_series, "macd_line": macd_line, "signal_line": signal_line,
        "bb_upper": bb_upper, "bb_lower": bb_lower, "sma_50": sma50, "sma_200": sma200,
    }

    # Find all occurrences for each setup
    results = {}
    for setup_id, setup_def in CANDIDATE_SETUPS.items():
        occurrences = []
        fn = setup_def["fn"]
        for i in range(210, len(closes) - max(FORWARD_HORIZONS)):
            if fn(state, i):
                regime = classify_regime(closes, sma200, i)
                fwd = _forward_returns(closes, i)
                if fwd:  # only keep if we have forward data
                    occ = PatternOccurrence(
                        setup_id=setup_id,
                        date=candles[i].date,
                        index=i,
                        price=closes[i],
                        regime=regime,
                        forward_returns=fwd,
                    )
                    occurrences.append(occ)
        results[setup_id] = occurrences

    return {
        "asset": asset,
        "timeframe": timeframe,
        "total_candles": len(candles),
        "analysis_window": f"{candles[210].date} to {candles[-1].date}",
        "results": results,
        "candidate_count": len(CANDIDATE_SETUPS),
    }


def validate_patterns(discovery: dict, min_samples: int = 30,
                      min_win_rate: float = 0.55,
                      setup_registry: dict = None) -> list:
    """M92 Falsification: filter patterns by statistical significance.

    A pattern survives only if:
      - >= min_samples occurrences (statistical power)
      - > min_win_rate at the 7-day horizon (predictive edge)
      - positive expectancy (avg win > avg loss in absolute terms)

    Args:
        setup_registry: which registry to look up setup defs in
                       (CANDIDATE_SETUPS for Phase 2, CONFLUENCE_SETUPS for Phase 3)

    Returns list of surviving patterns with full stats, sorted by win rate.
    """
    if setup_registry is None:
        setup_registry = CANDIDATE_SETUPS
    surviving = []
    for setup_id, occurrences in discovery["results"].items():
        if not occurrences:
            continue
        setup_def = setup_registry[setup_id]
        direction = setup_def["direction"]

        # Aggregate by regime at 7-day horizon
        regime_stats = {}
        for regime in ("bull", "bear", "chop", "unknown"):
            regime_occs = [o for o in occurrences if o.regime == regime]
            if not regime_occs:
                continue
            # For long setups, "win" = forward return > 0
            # For short setups, "win" = forward return < 0
            wins = 0
            returns_7d = []
            for o in regime_occs:
                r = o.forward_returns.get(7)
                if r is None:
                    continue
                returns_7d.append(r)
                if (direction == "long" and r > 0) or (direction == "short" and r < 0):
                    wins += 1
            if not returns_7d:
                continue
            win_rate = wins / len(returns_7d)
            avg_return = sum(returns_7d) / len(returns_7d)
            # Expectancy: for longs, positive is good; for shorts, negative is good
            signed_expectancy = avg_return if direction == "long" else -avg_return
            regime_stats[regime] = {
                "samples": len(returns_7d),
                "win_rate": round(win_rate, 3),
                "avg_return_7d": round(avg_return, 4),
                "expectancy": round(signed_expectancy, 4),
                "positive": signed_expectancy > 0,
            }

        # Overall stats (all regimes combined)
        all_returns_7d = [o.forward_returns.get(7) for o in occurrences
                          if o.forward_returns.get(7) is not None]
        if len(all_returns_7d) < min_samples:
            # Not enough samples — record but don't promote to surviving
            surviving.append({
                "setup_id": setup_id,
                "description": setup_def["desc"],
                "direction": direction,
                "total_samples": len(all_returns_7d),
                "overall_win_rate": None,
                "regime_stats": regime_stats,
                "survives": False,
                "reason": f"insufficient samples ({len(all_returns_7d)} < {min_samples})",
            })
            continue

        overall_wins = sum(
            1 for r in all_returns_7d
            if (direction == "long" and r > 0) or (direction == "short" and r < 0)
        )
        overall_win_rate = overall_wins / len(all_returns_7d)
        overall_avg = sum(all_returns_7d) / len(all_returns_7d)
        signed_overall = overall_avg if direction == "long" else -overall_avg

        survives = overall_win_rate > min_win_rate and signed_overall > 0

        surviving.append({
            "setup_id": setup_id,
            "description": setup_def["desc"],
            "direction": direction,
            "total_samples": len(all_returns_7d),
            "overall_win_rate": round(overall_win_rate, 3),
            "overall_avg_return_7d": round(overall_avg, 4),
            "overall_expectancy": round(signed_overall, 4),
            "regime_stats": regime_stats,
            "survives": survives,
            "reason": "PASS" if survives else (
                f"win_rate {overall_win_rate:.1%} <= {min_win_rate:.0%} or non-positive expectancy"
            ),
        })

    # Sort: surviving first, then by win rate
    surviving.sort(key=lambda x: (not x["survives"],
                                   -(x.get("overall_win_rate") or 0)))
    return surviving


def print_pattern_report(asset: str, timeframe: str, validated: list):
    """Print honest pattern validation results."""
    print("=" * 64)
    print(f" PATTERN VALIDATION: {asset} {timeframe}")
    print(f" (M51 extraction + M76 recurrence + M92 falsification)")
    print("=" * 64)
    print()

    survivors = [p for p in validated if p["survives"]]
    rejected = [p for p in validated if not p["survives"]]

    if survivors:
        print(f" SURVIVING PATTERNS ({len(survivors)}) - statistical edge found:")
        for p in survivors:
            print(f"   + {p['setup_id']:<20} win_rate={p['overall_win_rate']:.1%}  "
                  f"avg_7d={p['overall_avg_return_7d']:+.2%}  "
                  f"samples={p['total_samples']}  [{p['direction']}]")
            # Show regime breakdown
            for regime, stats in p["regime_stats"].items():
                if stats["samples"] >= 10:
                    tag = "EDGE" if stats["positive"] else "fails"
                    print(f"       {regime:>5}: win={stats['win_rate']:.1%} "
                          f"exp={stats['expectancy']:+.2%} "
                          f"n={stats['samples']} [{tag}]")
        print()

    if rejected:
        print(f" REJECTED PATTERNS ({len(rejected)}) - no statistical edge:")
        for p in rejected:
            wr = p.get("overall_win_rate")
            wr_str = f"{wr:.1%}" if wr is not None else "n/a"
            print(f"   - {p['setup_id']:<20} win_rate={wr_str}  "
                  f"samples={p['total_samples']}  ({p['reason']})")
        print()

    print("=" * 64)


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 4: Walk-Forward Validation
# Engages M96 (Window-Bootstrap Reasoning), M92 (Falsification), M73 (State-Change)
#
# The brutal honest test. Phase 2-3 results were IN-SAMPLE: computed over the
# full 2018-2026 history. That can mislead - a pattern might look great because
# one lucky era dominates. Walk-forward validation asks the harder questions:
#
#   1. Does the edge hold in MOST non-overlapping time windows, or just one?
#   2. Is the win rate statistically significant (bootstrap CI excludes 50%)?
#   3. Does the pattern BEAT buy-and-hold? (most TA patterns fail this)
#   4. What is the max drawdown? (a 91% win rate can still be untradeable)
#
# If a pattern fails ANY of these, it is not real edge - it is overfit.
# ═══════════════════════════════════════════════════════════════════════════════

import random as _random


def _bootstrap_ci(outcomes: list, n_boot: int = 10000, confidence: float = 0.95):
    """Bootstrap confidence interval on the mean of a binary outcome list.

    outcomes: list of 0/1 (loss/win)
    Returns (mean, ci_low, ci_high) at the given confidence level.
    """
    if not outcomes:
        return (None, None, None)
    n = len(outcomes)
    actual_mean = sum(outcomes) / n
    if n < 5:
        return (actual_mean, None, None)
    boot_means = []
    for _ in range(n_boot):
        sample = [_random.choice(outcomes) for _ in range(n)]
        boot_means.append(sum(sample) / n)
    boot_means.sort()
    alpha = (1 - confidence) / 2
    lo_idx = int(alpha * n_boot)
    hi_idx = int((1 - alpha) * n_boot)
    return (round(actual_mean, 4),
            round(boot_means[lo_idx], 4),
            round(boot_means[hi_idx], 4))


def _max_drawdown(returns: list) -> float:
    """Compute maximum drawdown from a list of period returns.

    returns: list of decimal returns (e.g., 0.05 = +5%)
    Returns max drawdown as a positive decimal (e.g., 0.40 = -40% peak).
    """
    if not returns:
        return 0.0
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for r in returns:
        equity *= (1 + r)
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak
        if dd > max_dd:
            max_dd = dd
    return round(max_dd, 4)


def walk_forward_validate(engine: "TAEngine", setup_id: str,
                          asset: str, timeframe: str,
                          window_years: float = 2.0,
                          horizon: int = 7,
                          min_window_samples: int = 5) -> dict:
    """Walk-forward validation of a single pattern.

    Splits history into non-overlapping windows, computes the pattern's
    win rate + expectancy in each window independently. A real edge
    persists across windows; an overfit edge clusters in one era.

    Also computes:
      - Bootstrap 95% CI on overall win rate (statistical significance)
      - Buy-and-hold baseline return over same period
      - Pattern's compounded return vs buy-and-hold
      - Maximum drawdown of pattern vs buy-and-hold

    Args:
        setup_id: pattern id from CONFLUENCE_SETUPS
        window_years: size of each walk-forward window
        horizon: forward-return horizon in candles
        min_window_samples: a window below this occurrence count is too
            small for its win_rate to mean anything (one lucky trade can
            swing it from 0% to 100%) and is excluded from the
            windows_passing_55pct verdict, though it is still reported in
            full in `windows` for transparency.
    """
    candles = engine._get_candles(asset, timeframe)
    if len(candles) < 400:
        return {"error": f"Insufficient data: {len(candles)}"}

    closes = [c.close for c in candles]
    volumes = [c.volume for c in candles]
    highs = [c.high for c in candles]
    lows = [c.low for c in candles]

    # Indicator state (reuse Phase 2/3 computations)
    rsi_series = rsi(closes, 14)
    macd_line, signal_line, _ = macd(closes)
    _, bb_upper, bb_lower = bollinger_bands(closes, 20, 2.0)
    sma50 = sma(closes, 50)
    sma200 = sma(closes, 200)
    state = {
        "closes": closes, "highs": highs, "lows": lows, "volumes": volumes,
        "rsi_14": rsi_series, "macd_line": macd_line, "signal_line": signal_line,
        "bb_upper": bb_upper, "bb_lower": bb_lower, "sma_50": sma50, "sma_200": sma200,
    }

    # Get the setup function
    if setup_id in CONFLUENCE_SETUPS:
        setup_def = CONFLUENCE_SETUPS[setup_id]
    elif setup_id in CANDIDATE_SETUPS:
        setup_def = CANDIDATE_SETUPS[setup_id]
    else:
        return {"error": f"Unknown setup: {setup_id}"}
    fn = setup_def["fn"]
    direction = setup_def["direction"]

    # Find ALL occurrences with forward returns at the chosen horizon
    occurrences = []
    for i in range(210, len(closes) - horizon):
        if fn(state, i):
            fwd = (closes[i + horizon] - closes[i]) / closes[i]
            # Win depends on direction
            win = 1 if (direction == "long" and fwd > 0) or \
                       (direction == "short" and fwd < 0) else 0
            occurrences.append({
                "index": i,
                "date": candles[i].date,
                "forward_return": fwd,
                "win": win,
                "price": closes[i],
            })

    if not occurrences:
        return {"error": f"No occurrences of {setup_id} found"}

    # Split into windows by calendar time
    # Each window = window_years worth of occurrences
    window_days = int(window_years * 365)
    windows = []
    if occurrences:
        start_idx = occurrences[0]["index"]
        current_window = []
        for occ in occurrences:
            candle_idx = occ["index"]
            days_elapsed = (candles[candle_idx].open_time -
                            candles[start_idx].open_time) / (1000 * 86400)
            if days_elapsed > window_days and current_window:
                windows.append(current_window)
                current_window = [occ]
                # Reset the start to this occurrence for next window
                start_idx = candle_idx
            else:
                current_window.append(occ)
        if current_window:
            windows.append(current_window)

    # Per-window stats
    window_stats = []
    for w in windows:
        wins = sum(o["win"] for o in w)
        win_rate = wins / len(w) if w else 0
        avg_return = sum(o["forward_return"] for o in w) / len(w)
        # Signed expectancy (positive = good for the direction)
        signed = avg_return if direction == "long" else -avg_return
        window_stats.append({
            "samples": len(w),
            "win_rate": round(win_rate, 3),
            "avg_return": round(avg_return, 4),
            "expectancy": round(signed, 4),
            "first_date": w[0]["date"],
            "last_date": w[-1]["date"],
        })

    # Overall bootstrap CI on win rate
    all_outcomes = [o["win"] for o in occurrences]
    mean_wr, ci_lo, ci_hi = _bootstrap_ci(all_outcomes, n_boot=10000)

    # Pattern compounded return (long direction: hold for `horizon` after each signal)
    # For shorts, returns are inverted
    pattern_returns = []
    for o in occurrences:
        r = o["forward_return"] if direction == "long" else -o["forward_return"]
        pattern_returns.append(r)

    # Buy-and-hold baseline: same number of `horizon`-period holds
    # Pick random entry points matching the pattern's occurrence count
    # for fair comparison
    n_signals = len(occurrences)
    bh_returns = []
    for o in occurrences:
        # Buy-and-hold return over the SAME window as this signal
        bh_returns.append(o["forward_return"])

    # For long direction, pattern returns = bh returns (same direction)
    # The fair comparison is: signal-filtered returns vs ALL periods
    all_period_returns = []
    for i in range(210, len(closes) - horizon):
        all_period_returns.append((closes[i + horizon] - closes[i]) / closes[i])

    pattern_compounded = 1.0
    for r in pattern_returns:
        pattern_compounded *= (1 + r)

    bh_compounded = 1.0
    for r in all_period_returns:
        bh_compounded *= (1 + r)

    pattern_dd = _max_drawdown(pattern_returns)
    bh_dd = _max_drawdown(all_period_returns)

    # Verdict logic. A window below min_window_samples proves nothing --
    # one lucky (or unlucky) trade can swing win_rate from 0% to 100%, and
    # unlike validate_patterns() (min_samples=30 on the whole dataset) this
    # loop previously had no floor at all. Only windows with enough
    # occurrences to be minimally informative count toward the verdict;
    # every window is still reported in full in `windows` above.
    eligible_windows = [
        w for w in window_stats if w["samples"] >= min_window_samples
    ]
    windows_passing = sum(
        1 for w in eligible_windows if w["win_rate"] > 0.55
    )
    windows_total = len(eligible_windows)
    windows_excluded_insufficient_samples = (
        len(window_stats) - windows_total
    )
    ci_significant = ci_lo is not None and ci_lo > 0.50
    beats_bh = pattern_compounded > bh_compounded

    if windows_passing >= max(1, windows_total * 0.6) and ci_significant and beats_bh:
        verdict = "REAL EDGE - survives walk-forward, CI excludes 50%, beats buy-and-hold"
    elif windows_passing >= max(1, windows_total * 0.6) and ci_significant:
        verdict = "STAT-SIG BUT UNDERPERFORMS BUY-AND-HOLD - not practically useful"
    elif ci_significant:
        verdict = "WEAK - significant overall but inconsistent across windows"
    else:
        verdict = "FAILS - no statistical edge after walk-forward"

    return {
        "asset": asset,
        "timeframe": timeframe,
        "setup_id": setup_id,
        "description": setup_def["desc"],
        "direction": direction,
        "horizon_candles": horizon,
        "total_occurrences": len(occurrences),
        "overall_win_rate": mean_wr,
        "win_rate_ci_95": [ci_lo, ci_hi],
        "ci_significant": ci_significant,
        "windows": window_stats,
        "windows_passing_55pct": windows_passing,
        "windows_total": windows_total,
        "min_window_samples": min_window_samples,
        "windows_excluded_insufficient_samples": (
            windows_excluded_insufficient_samples
        ),
        "pattern_compounded_return": round(pattern_compounded, 4),
        "buyhold_compounded_return": round(bh_compounded, 4),
        "beats_buyhold": beats_bh,
        "pattern_max_drawdown": pattern_dd,
        "buyhold_max_drawdown": bh_dd,
        "verdict": verdict,
    }


def print_walk_forward_report(result: dict):
    """Print walk-forward validation results honestly."""
    if "error" in result:
        print(f" ERROR: {result['error']}")
        return

    print("=" * 64)
    print(f" WALK-FORWARD VALIDATION")
    print(f" {result['asset']} {result['timeframe']} | {result['setup_id']}")
    print(f" ({result['description']})")
    print("=" * 64)
    print()
    print(f" Total occurrences: {result['total_occurrences']}  "
          f"(horizon: {result['horizon_candles']} candles)")
    print(f" Overall win rate:  {result['overall_win_rate']:.1%}")
    ci = result["win_rate_ci_95"]
    if ci[0] is not None:
        print(f" 95% bootstrap CI:  [{ci[0]:.1%}, {ci[1]:.1%}]  "
              f"{'SIGNIFICANT' if result['ci_significant'] else 'NOT SIG (includes 50%)'}")
    print()

    min_n = result["min_window_samples"]
    print(f" Walk-forward windows ({len(result['windows'])} windows, "
          f"{result['windows_total']} with >={min_n} samples counted "
          f"toward the verdict):")
    for w in result["windows"]:
        bar_len = int(w["win_rate"] * 20)
        bar = "#" * bar_len + "." * (20 - bar_len)
        if w["samples"] < min_n:
            tag = "thin"  # too few samples to count toward the verdict
        else:
            tag = "PASS" if w["win_rate"] > 0.55 else "fail"
        print(f"   {w['first_date']} to {w['last_date']}: "
              f"win={w['win_rate']:.1%} [{bar}] "
              f"exp={w['expectancy']:+.2%} n={w['samples']} [{tag}]")
    wp = result["windows_passing_55pct"]
    wt = result["windows_total"]
    print(f" Passing windows: {wp}/{wt} "
          f"({wp/wt:.0%})" if wt else " Passing: 0/0")
    print()

    print(f" Buy-and-hold comparison:")
    print(f"   Pattern compounded:     {result['pattern_compounded_return']:.2f}x  "
          f"(max DD: {result['pattern_max_drawdown']:.1%})")
    print(f"   Buy-and-hold compounded: {result['buyhold_compounded_return']:.2f}x  "
          f"(max DD: {result['buyhold_max_drawdown']:.1%})")
    print(f"   Pattern beats BH:       {result['beats_buyhold']}")
    print()

    # Verdict with appropriate icon
    v = result["verdict"]
    if v.startswith("REAL"):
        icon = "[REAL EDGE]"
    elif v.startswith("STAT"):
        icon = "[STAT-SIG ONLY]"
    elif v.startswith("WEAK"):
        icon = "[WEAK]"
    else:
        icon = "[FAILS]"
    print(f" VERDICT: {icon}")
    print(f"   {v}")
    print("=" * 64)


# ═══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    import argparse
    p = argparse.ArgumentParser(description="Chainseer TA - Technical Analysis")
    p.add_argument("asset", nargs="?", default="BTC",
                   help="Asset: BTC or ETH (default: BTC)")
    p.add_argument("--timeframe", "-t", default="1d",
                   help="Timeframe: 1d or 4h (default: 1d)")
    p.add_argument("--both", action="store_true",
                   help="Analyze both BTC and ETH")
    p.add_argument("--refresh", action="store_true",
                   help="Force refetch (ignore cache)")
    p.add_argument("--discover", action="store_true",
                   help="Run pattern discovery + validation (Phase 2)")
    p.add_argument("--confluence", action="store_true",
                   help="Run confluence pattern discovery (Phase 3)")
    p.add_argument("--lead-lag", action="store_true",
                   help="Analyze BTC->ETH lead-lag relationship (Phase 3)")
    p.add_argument("--walk-forward", metavar="SETUP", default=None,
                   help="Walk-forward validate a setup (Phase 4), e.g. regime_chop_volume")
    args = p.parse_args()

    engine = TAEngine()

    assets = ["BTC", "ETH"] if args.both else [args.asset.upper()]
    for asset in assets:
        if asset not in SUPPORTED_ASSETS:
            print(f"Unsupported asset: {asset}. Use BTC or ETH.")
            continue
        print(f"\nFetching {asset} {args.timeframe} history...")
        if args.refresh and asset == assets[0]:
            # Clear cache for refresh
            cache_file = CACHE_DIR / f"{asset}_{args.timeframe}.json"
            if cache_file.exists():
                cache_file.unlink()
        engine.fetch_history(asset, args.timeframe)
        if args.discover:
            # Phase 2: pattern discovery + validation
            print(f"\nDiscovering patterns (M51 + M76)...")
            discovery = discover_patterns(engine, asset, args.timeframe)
            if "error" in discovery:
                print(f" ERROR: {discovery['error']}")
                continue
            print(f"  Analysis window: {discovery['analysis_window']}")
            print(f"  Validating (M92 falsification)...")
            validated = validate_patterns(discovery)
            print_pattern_report(asset, args.timeframe, validated)

            # Save full results
            out_file = f"ta_patterns_{asset}_{args.timeframe}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump({
                    "asset": asset,
                    "timeframe": args.timeframe,
                    "analysis_window": discovery["analysis_window"],
                    "total_candles": discovery["total_candles"],
                    "validated_patterns": validated,
                }, f, indent=2, default=str)
            print(f"  Full results saved to: {out_file}")
        elif args.confluence:
            # Phase 3: confluence pattern discovery
            print(f"\nDiscovering confluence patterns (M94)...")
            discovery = discover_confluence(engine, asset, args.timeframe)
            if "error" in discovery:
                print(f" ERROR: {discovery['error']}")
                continue
            print(f"  Analysis window: {discovery['analysis_window']}")
            print(f"  Validating (M92 falsification)...")
            validated = validate_patterns(discovery, setup_registry=CONFLUENCE_SETUPS)
            print_pattern_report(asset, args.timeframe, validated)

            out_file = f"ta_confluence_{asset}_{args.timeframe}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump({
                    "asset": asset,
                    "timeframe": args.timeframe,
                    "phase": "confluence",
                    "analysis_window": discovery["analysis_window"],
                    "total_candles": discovery["total_candles"],
                    "validated_patterns": validated,
                }, f, indent=2, default=str)
            print(f"  Full results saved to: {out_file}")
        elif args.lead_lag:
            # Phase 3: BTC->ETH lead-lag (needs both assets loaded)
            if asset != "BTC":
                continue
            print(f"\nEnsuring ETH data loaded...")
            engine.fetch_history("ETH", args.timeframe, verbose=False)
            print(f"Analyzing BTC->ETH lead-lag (M94 cross-asset)...")
            result = analyze_lead_lag(engine, args.timeframe)
            if "error" in result:
                print(f" ERROR: {result['error']}")
                continue
            print()
            print("=" * 64)
            print(f" BTC->ETH LEAD-LAG ANALYSIS ({args.timeframe})")
            print(f" Threshold: BTC moves >= {result['threshold_pct']}%")
            print(f" BTC triggers: {result['total_btc_moves']}  "
                  f"Window: {result['alignment_window']}")
            print("=" * 64)
            for direction in ("up", "down"):
                d = result["stats"][direction]
                print(f"\n BTC {direction.upper()} moves (n={d['count']}):")
                print(f"   ETH follow rate by lag (candles):")
                for lag in sorted(d["eth_follow_rate"].keys(),
                                  key=lambda x: int(x)):
                    r = d["eth_follow_rate"][lag]
                    if r["follow_rate"] is not None:
                        bar_len = int(r["follow_rate"] * 30)
                        bar = "#" * bar_len + "." * (30 - bar_len)
                        print(f"     +{lag:>2}: {r['follow_rate']:.1%} "
                              f"[{bar}] n={r['samples']}")
            print()
            print("=" * 64)

            out_file = f"ta_lead_lag_{args.timeframe}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, default=str)
            print(f" Full results saved to: {out_file}")
        elif args.walk_forward:
            # Phase 4: walk-forward validation of a specific pattern
            print(f"\nWalk-forward validating: {args.walk_forward} (M96)...")
            result = walk_forward_validate(engine, args.walk_forward,
                                           asset, args.timeframe)
            print_walk_forward_report(result)
            if "error" not in result:
                out_file = f"ta_walkforward_{args.walk_forward}_{asset}_{args.timeframe}.json"
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(result, f, indent=2, default=str)
                print(f" Full results saved to: {out_file}")
        else:
            analysis = engine.analyze(asset, args.timeframe)
            engine.print_report(analysis)


if __name__ == "__main__":
    main()
