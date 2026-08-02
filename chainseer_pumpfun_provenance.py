"""Pump.fun on-chain launch provenance: CreateEvent decoding and creator
deployment-cadence scanning.

Shared, stateless building block for anything that needs to answer "was
this mint launched on Pump.fun, and by whom, and how often has that
creator launched tokens recently" -- currently used by
chainseer_solana_public.py (the stateless public analyzer). Deliberately
does not depend on chainseer_solana.py's PumpFunObserver/SolanaRiskAnalyzer
(the paper-trading autotrader's own, independently-tested implementation of
the same on-chain decoding) -- these are pure functions over an injected
RPC-call interface, no local files, no catalog, no caching, no learner
state. Callers own persistence/caching if they want it.

Every function takes two plain callables rather than an RPC object, so it
never assumes a specific client class's method names:
  get_signatures(address, *, limit, before=None) -> list[dict]
  get_transaction(signature) -> dict | None

A get_transaction call that raises is treated the same as one that
returns None -- both simply mean "skip this one, keep going" for the
bounded scans below.
"""

from __future__ import annotations

import base64
import struct
import time
from dataclasses import dataclass

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
PUMP_CREATE_EVENT_DISCRIMINATOR = bytes([27, 114, 169, 77, 222, 235, 99, 118])

_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(raw: bytes) -> str:
    number = int.from_bytes(raw, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = _BASE58_ALPHABET[remainder] + encoded
    leading = len(raw) - len(raw.lstrip(b"\0"))
    return "1" * leading + (encoded or ("" if leading else "1"))


class _BorshReader:
    def __init__(self, raw: bytes):
        self.raw = raw
        self.offset = 0

    def take(self, size: int) -> bytes:
        end = self.offset + size
        if end > len(self.raw):
            raise ValueError("truncated Borsh payload")
        value = self.raw[self.offset:end]
        self.offset = end
        return value

    def u8(self) -> int:
        return self.take(1)[0]

    def u64(self) -> int:
        return struct.unpack("<Q", self.take(8))[0]

    def i64(self) -> int:
        return struct.unpack("<q", self.take(8))[0]

    def string(self, maximum: int = 4096) -> str:
        size = struct.unpack("<I", self.take(4))[0]
        if size > maximum:
            raise ValueError(f"Borsh string exceeds {maximum} bytes")
        return self.take(size).decode("utf-8", errors="replace")

    def pubkey(self) -> str:
        return _b58encode(self.take(32))


@dataclass(frozen=True)
class PumpFunCreateEvent:
    """Just enough of a Pump.fun CreateEvent to attribute a launch to its
    creator -- not the full launch-candidate shape chainseer_solana.py's
    autotrader needs (no reserves/token_program, since callers here only
    ever use this for provenance/creator-cadence, never for risk scoring
    the bonding curve itself)."""

    signature: str
    slot: int
    block_time: int
    name: str
    symbol: str
    mint: str
    bonding_curve: str
    user: str
    creator: str


def decode_create_event(
    encoded: str, *, signature: str, slot: int, block_time: int
) -> PumpFunCreateEvent | None:
    try:
        raw = base64.b64decode(encoded, validate=True)
        if not raw.startswith(PUMP_CREATE_EVENT_DISCRIMINATOR):
            return None
        reader = _BorshReader(raw[len(PUMP_CREATE_EVENT_DISCRIMINATOR):])
        name = reader.string(512)
        symbol = reader.string(128)
        reader.string(4096)  # uri, unused here
        mint = reader.pubkey()
        bonding_curve = reader.pubkey()
        user = reader.pubkey()
        creator = reader.pubkey()
        event_time = reader.i64()
        # virtual_token/virtual_quote/real_token/total_supply/token_program
        # and the optional mayhem/cashback tail all follow -- irrelevant for
        # provenance, deliberately not parsed further.
        return PumpFunCreateEvent(
            signature=signature,
            slot=slot,
            block_time=event_time or block_time,
            name=name,
            symbol=symbol,
            mint=mint,
            bonding_curve=bonding_curve,
            user=user,
            creator=creator,
        )
    except (ValueError, struct.error):
        return None


def decode_transaction(
    signature_row: dict, transaction: dict | None
) -> list[PumpFunCreateEvent]:
    if not transaction or (transaction.get("meta") or {}).get("err") is not None:
        return []
    message = ((transaction.get("transaction") or {}).get("message") or {})
    keys = message.get("accountKeys") or []
    addresses = {
        item.get("pubkey") if isinstance(item, dict) else item for item in keys
    }
    if PUMP_PROGRAM_ID not in addresses:
        return []
    output = []
    for log in (transaction.get("meta") or {}).get("logMessages") or []:
        if not isinstance(log, str) or not log.startswith("Program data: "):
            continue
        event = decode_create_event(
            log.split("Program data: ", 1)[1].strip(),
            signature=signature_row["signature"],
            slot=int(signature_row.get("slot") or 0),
            block_time=int(
                transaction.get("blockTime") or signature_row.get("blockTime") or 0
            ),
        )
        if event:
            output.append(event)
    return output


def _safe_get_transaction(get_transaction, signature: str):
    try:
        return get_transaction(signature)
    except Exception:
        return None


def resolve_genesis_creator(
    get_signatures,
    get_transaction,
    mint: str,
    *,
    max_pages: int = 40,
    page_size: int = 1000,
    decode_last: int = 10,
) -> PumpFunCreateEvent | None:
    """Best-effort resolution of a mint's original Pump.fun CreateEvent.

    Walks the mint account's own signature history backwards using cheap
    metadata-only pages (no transaction decode) until the final, smallest
    page is reached -- that page holds the mint's oldest signatures, and
    since the mint account did not exist before Pump's create instruction
    made it, the genesis transaction must be among the very oldest of them.
    Only those few are actually decoded.

    Bounded by max_pages so a long-lived, heavily-traded mint (almost
    certainly already graduated, possibly migrated off Pump.fun entirely)
    fails closed with None rather than replaying its full history.
    """
    last_batch: list[dict] = []
    before_signature: str | None = None
    for _ in range(max_pages):
        batch = get_signatures(mint, limit=page_size, before=before_signature)
        if not batch:
            break
        last_batch = batch
        if len(batch) < page_size:
            break
        before_signature = batch[-1].get("signature")
        if not before_signature:
            break
    else:
        return None

    for row in reversed(last_batch[-decode_last:]):
        if row.get("err"):
            continue
        transaction = _safe_get_transaction(get_transaction, row["signature"])
        if transaction is None:
            continue
        for event in decode_transaction(row, transaction):
            if event.mint == mint:
                return event
    return None


def creator_deployment_history(
    get_signatures,
    get_transaction,
    creator: str,
    *,
    exclude_mint: str | None = None,
    window_hours: float = 72.0,
    signature_page_size: int = 30,
    max_pages: int = 6,
    max_transactions_scanned: int = 180,
    now: float | None = None,
) -> dict:
    """Scan a creator wallet's recent Pump.fun deployment behaviour.

    Descriptive, not a verdict on any prior token: counts how many tokens
    the creator has deployed in the lookback window and how rapidly.
    Industrialized deployment cadence (many tokens in hours) is an
    objective, risk-correlated signal -- it does not require labelling any
    prior token a rug or a winner.

    getSignaturesForAddress returns EVERY signature for the wallet, not
    just Pump.fun interactions -- a creator who also trades on the same
    wallet can fill a single page with unrelated activity and hide genuine
    prior launches from a one-shot scan. Pages backwards until the lookback
    window is covered, bounded by max_pages/max_transactions_scanned as an
    RPC cost ceiling.
    """
    now_epoch = now if now is not None else time.time()
    window_seconds = window_hours * 3600.0
    cutoff_epoch = now_epoch - window_seconds

    result = {
        "creator": creator,
        "scanned": False,
        "scan_degraded": False,
        "transactions_failed": 0,
        "pages_scanned": 0,
        "signatures_scanned": 0,
        "prior_deployments_in_window": 0,
        "window_hours": window_hours,
        "deployments_per_hour": 0.0,
        "median_minutes_between_deployments": None,
        "prior_symbols_sample": [],
    }
    if not creator or len(creator) < 32:
        return result

    signatures: list[dict] = []
    before_signature: str | None = None
    pages_scanned = 0
    for _ in range(max_pages):
        batch = get_signatures(
            creator, limit=signature_page_size, before=before_signature
        )
        pages_scanned += 1
        if not batch:
            break
        signatures.extend(batch)
        oldest_row = batch[-1]
        before_signature = oldest_row.get("signature")
        if not before_signature or len(signatures) >= max_transactions_scanned:
            break
        oldest_time = oldest_row.get("blockTime")
        if oldest_time is not None and int(oldest_time) < cutoff_epoch:
            break
    result["pages_scanned"] = pages_scanned
    if not signatures:
        return result

    signatures = signatures[:max_transactions_scanned]
    result["signatures_scanned"] = len(signatures)

    prior_deployments: list[PumpFunCreateEvent] = []
    transactions_failed = 0
    for row in signatures:
        if row.get("err"):
            continue
        transaction = _safe_get_transaction(get_transaction, row["signature"])
        if transaction is None:
            transactions_failed += 1
            continue
        for event in decode_transaction(row, transaction):
            if event.creator == creator and event.mint != exclude_mint:
                prior_deployments.append(event)

    result["scanned"] = True
    result["scan_degraded"] = transactions_failed > 0
    result["transactions_failed"] = transactions_failed

    recent = [
        event
        for event in prior_deployments
        if (now_epoch - event.block_time) <= window_seconds
    ]
    result["prior_deployments_in_window"] = len(recent)
    if window_seconds > 0:
        result["deployments_per_hour"] = round(
            len(recent) / (window_seconds / 3600.0), 3
        )
    if len(recent) >= 2:
        times = sorted(event.block_time for event in recent)
        gaps = sorted(
            (times[i + 1] - times[i]) / 60.0 for i in range(len(times) - 1)
        )
        result["median_minutes_between_deployments"] = round(
            gaps[len(gaps) // 2], 1
        )
    result["prior_symbols_sample"] = [
        {"symbol": event.symbol, "mint": event.mint, "block_time": event.block_time}
        for event in sorted(recent, key=lambda e: e.block_time, reverse=True)[:10]
    ]
    return result
