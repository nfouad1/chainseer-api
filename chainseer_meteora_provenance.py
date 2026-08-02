"""Meteora on-chain launch provenance: Dynamic Bonding Curve (DBC) pool-
creation attribution and deployer-cadence scanning.

Shared, stateless building block for anything that needs to answer "was
this mint launched via Meteora's Dynamic Bonding Curve, and by whom, and
how often has that creator launched tokens recently" -- the Meteora analog
of chainseer_pumpfun_provenance.py, built the same way: pure functions over
an injected RPC-call interface, no local files, no catalog, no learner
state. Callers own persistence/caching if they want it.

Program IDs and the DBC pool-creation instruction layout below were
verified directly against MeteoraAg's own source (not guessed, not taken
from an unverified summary):
  - Program IDs cross-checked against the "Program Address"/"Deployments"
    sections of https://github.com/MeteoraAg/dynamic-bonding-curve and
    https://github.com/MeteoraAg/damm-v2 READMEs, and the Solscan account
    pages for the DLMM program.
  - The three pool-creation instructions that mint a brand-new base token
    (initialize_virtual_pool_with_spl_token, ..._with_token2022, and
    ..._with_token2022_transfer_hook) were confirmed by name in
    dynamic-bonding-curve's lib.rs #[program] module, and their Anchor
    global-dispatch discriminators (first 8 bytes of
    sha256(b"global:<instruction_name>")) were computed directly here,
    not sourced from a third party.
  - Each instruction's #[derive(Accounts)] struct was read from its own
    source file; all three place `creator` at account index 2 and
    `base_mint` at account index 3 (Anchor fixes account order at compile
    time to struct declaration order), so attribution is identical
    regardless of which variant fired.

Every function takes two plain callables rather than an RPC object, so it
never assumes a specific client class's method names:
  get_signatures(address, *, limit, before=None) -> list[dict]
  get_transaction(signature) -> dict | None

Transactions are expected in Solana's "jsonParsed" getTransaction shape:
for a program the RPC node has no built-in parser for (true of DBC on any
generic RPC), each instruction comes back as a "partially decoded"
instruction with `programId`, `accounts` (already-resolved pubkey
strings, not accountKeys indices), and `data` (base58) -- this is standard
jsonParsed behavior, not specific to this module.

A get_transaction call that raises is treated the same as one that
returns None -- both simply mean "skip this one, keep going" for the
bounded scans below.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

DBC_PROGRAM_ID = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN"
DAMM_V2_PROGRAM_ID = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"
DLMM_PROGRAM_ID = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"

# Anchor global-dispatch discriminators for the pool-creation instructions,
# computed as the first 8 bytes of sha256(b"global:<instruction_name>")
# for each name confirmed in dynamic-bonding-curve's lib.rs:
#   initialize_virtual_pool_with_spl_token                -> 8c55d7b06636684f
#   initialize_virtual_pool_with_token2022                -> a976334e916edc9b
#   initialize_virtual_pool_with_token2022_transfer_hook  -> b60de9b12a918702
_DBC_POOL_CREATION_DISCRIMINATORS = {
    bytes.fromhex("8c55d7b06636684f"): "spl_token",
    bytes.fromhex("a976334e916edc9b"): "token2022",
    bytes.fromhex("b60de9b12a918702"): "token2022_transfer_hook",
}
_DBC_CREATOR_ACCOUNT_INDEX = 2
_DBC_BASE_MINT_ACCOUNT_INDEX = 3
_DBC_POOL_ACCOUNT_INDEX = 5

_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_INDEX = {char: index for index, char in enumerate(_BASE58_ALPHABET)}


def _b58decode(value: str) -> bytes:
    number = 0
    for char in value:
        number = number * 58 + _BASE58_INDEX[char]
    raw = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    leading = len(value) - len(value.lstrip("1"))
    return b"\0" * leading + raw


@dataclass(frozen=True)
class MeteoraPoolCreation:
    """Just enough of a DBC pool-creation instruction to attribute a launch
    to its creator -- mirrors chainseer_pumpfun_provenance.PumpFunCreateEvent's
    scope (provenance/creator-cadence only, never full risk scoring). No
    name/symbol: those are set by a separate metadata instruction whose
    layout this module does not decode, so none is claimed here."""

    signature: str
    slot: int
    block_time: int
    mint: str
    creator: str
    pool: str
    variant: str


def _decode_pool_creation_instruction(
    instruction: dict,
) -> tuple[str, str, str, str] | None:
    """Returns (creator, base_mint, pool, variant) if `instruction` is a DBC
    pool-creation call, else None. Only inspects account POSITIONS (fixed
    by Anchor at compile time) -- never decodes the instruction's argument
    payload, which isn't needed for attribution."""
    if instruction.get("programId") != DBC_PROGRAM_ID:
        return None
    data = instruction.get("data")
    accounts = instruction.get("accounts") or []
    if not isinstance(data, str) or len(accounts) <= _DBC_POOL_ACCOUNT_INDEX:
        return None
    try:
        raw = _b58decode(data)
    except KeyError:
        return None
    variant = _DBC_POOL_CREATION_DISCRIMINATORS.get(raw[:8])
    if variant is None:
        return None
    creator = accounts[_DBC_CREATOR_ACCOUNT_INDEX]
    base_mint = accounts[_DBC_BASE_MINT_ACCOUNT_INDEX]
    pool = accounts[_DBC_POOL_ACCOUNT_INDEX]
    if not all(isinstance(v, str) for v in (creator, base_mint, pool)):
        return None
    return creator, base_mint, pool, variant


def decode_transaction(
    signature_row: dict, transaction: dict | None
) -> list[MeteoraPoolCreation]:
    if not transaction or (transaction.get("meta") or {}).get("err") is not None:
        return []
    message = (transaction.get("transaction") or {}).get("message") or {}
    instructions = list(message.get("instructions") or [])
    for inner in (transaction.get("meta") or {}).get("innerInstructions") or []:
        instructions.extend(inner.get("instructions") or [])
    output = []
    for instruction in instructions:
        if not isinstance(instruction, dict):
            continue
        decoded = _decode_pool_creation_instruction(instruction)
        if decoded is None:
            continue
        creator, base_mint, pool, variant = decoded
        output.append(
            MeteoraPoolCreation(
                signature=signature_row["signature"],
                slot=int(signature_row.get("slot") or 0),
                block_time=int(
                    transaction.get("blockTime")
                    or signature_row.get("blockTime")
                    or 0
                ),
                mint=base_mint,
                creator=creator,
                pool=pool,
                variant=variant,
            )
        )
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
) -> MeteoraPoolCreation | None:
    """Best-effort resolution of a mint's originating DBC pool-creation
    instruction. Mirrors chainseer_pumpfun_provenance.resolve_genesis_creator
    exactly: walk the mint account's own signature history backwards using
    cheap metadata-only pages until the final, smallest page (the mint's
    oldest signatures) is reached, then only decode those few -- the pool-
    creation instruction that minted this token must be among them, since
    the mint account did not exist before it ran.

    Bounded by max_pages so a long-lived mint fails closed with None rather
    than replaying its full history.
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
        for creation in decode_transaction(row, transaction):
            if creation.mint == mint:
                return creation
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
    """Scan a creator wallet's recent Meteora DBC deployment behaviour.

    Descriptive, not a verdict on any prior token -- see
    chainseer_pumpfun_provenance.creator_deployment_history, whose
    cadence-scanning rationale and bounded-pagination approach this
    mirrors exactly, just decoding DBC pool-creation instructions instead
    of Pump.fun CreateEvents.
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
        "prior_launches_sample": [],
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

    prior_deployments: list[MeteoraPoolCreation] = []
    transactions_failed = 0
    for row in signatures:
        if row.get("err"):
            continue
        transaction = _safe_get_transaction(get_transaction, row["signature"])
        if transaction is None:
            transactions_failed += 1
            continue
        for creation in decode_transaction(row, transaction):
            if creation.creator == creator and creation.mint != exclude_mint:
                prior_deployments.append(creation)

    result["scanned"] = True
    result["scan_degraded"] = transactions_failed > 0
    result["transactions_failed"] = transactions_failed

    recent = [
        creation
        for creation in prior_deployments
        if (now_epoch - creation.block_time) <= window_seconds
    ]
    result["prior_deployments_in_window"] = len(recent)
    if window_seconds > 0:
        result["deployments_per_hour"] = round(
            len(recent) / (window_seconds / 3600.0), 3
        )
    if len(recent) >= 2:
        times = sorted(creation.block_time for creation in recent)
        gaps = sorted(
            (times[i + 1] - times[i]) / 60.0 for i in range(len(times) - 1)
        )
        result["median_minutes_between_deployments"] = round(
            gaps[len(gaps) // 2], 1
        )
    result["prior_launches_sample"] = [
        {"mint": creation.mint, "block_time": creation.block_time}
        for creation in sorted(recent, key=lambda e: e.block_time, reverse=True)[:10]
    ]
    return result
