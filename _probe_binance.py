"""Probe Binance klines API to ground the TA pipeline design."""
import requests
import json
import hashlib
from datetime import datetime, timezone

url = 'https://api.binance.com/api/v3/klines'
params = {'symbol': 'BTCUSDT', 'interval': '1d', 'limit': 5}
r = requests.get(url, params=params, timeout=15)
print(f'Status: {r.status_code}')
data = r.json()
print(f'Response shape: list of {len(data)} candles')
print(f'Candle format (first): {data[0]}')
print()

# Binance kline: [openTime, open, high, low, close, volume,
#                 closeTime, quoteVolume, trades, takerBuyBase, takerBuyQuote, ignore]
c = data[0]
print('Decoded first candle:')
print(f'  Open time:  {datetime.fromtimestamp(c[0]/1000, tz=timezone.utc).isoformat()}')
print(f'  Open:       ${float(c[1]):,.2f}')
print(f'  High:       ${float(c[2]):,.2f}')
print(f'  Low:        ${float(c[3]):,.2f}')
print(f'  Close:      ${float(c[4]):,.2f}')
print(f'  Volume:     {float(c[5]):,.0f} BTC')
print(f'  Close time: {datetime.fromtimestamp(c[6]/1000, tz=timezone.utc).isoformat()}')
print(f'  Quote vol:  ${float(c[7]):,.0f}')
print(f'  Trades:     {c[8]:,}')
print()

# Provenance test: hash the raw response
resp_hash = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
print(f'Provenance: SHA-256 of raw response = {resp_hash[:32]}...')

# How deep can we go? Fetch earliest BTC candle
params2 = {'symbol': 'BTCUSDT', 'interval': '1d', 'limit': 1, 'startTime': 0}
r2 = requests.get(url, params=params2, timeout=15)
earliest = r2.json()[0]
print()
print(f'Earliest BTC candle on Binance: {datetime.fromtimestamp(earliest[0]/1000, tz=timezone.utc).date()}')
print(f'  Open: ${float(earliest[1]):,.2f}')

# ETH
params3 = {'symbol': 'ETHUSDT', 'interval': '1d', 'limit': 1, 'startTime': 0}
r3 = requests.get(url, params=params3, timeout=15)
if r3.status_code == 200:
    eth_earliest = r3.json()[0]
    print(f'Earliest ETH candle on Binance: {datetime.fromtimestamp(eth_earliest[0]/1000, tz=timezone.utc).date()}')
    print(f'  Open: ${float(eth_earliest[1]):,.2f}')

# 4h candle test
params4 = {'symbol': 'BTCUSDT', 'interval': '4h', 'limit': 3}
r4 = requests.get(url, params=params4, timeout=15)
print()
print(f'4h candle test: {len(r4.json())} candles, latest close: ${float(r4.json()[-1][4]):,.2f}')
print(f'  4h candle open time: {datetime.fromtimestamp(r4.json()[0][0]/1000, tz=timezone.utc).isoformat()}')
