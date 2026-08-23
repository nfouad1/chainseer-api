"""Shared spec builder for the engine-level deferred-sealing tests."""
from chainseer_robinhood_gate import build_commitment_spec


def commit_spec(**overrides) -> dict:
    base = build_commitment_spec(
        run_id="run-e2e", network="robinhood",
        token_address="0xBB" + "0" * 38,
        evidence={"window": 2}, evidence_block_pin=2000,
        quote={"price": 3.5}, quote_block=2001, decision="buy_eligible",
        hard_stops=["liquidity_floor"], policy_version="pv-e2e",
        faculty_registry_epoch="epoch-7",
        verified_head={"head_index": 1, "head_hash": "0xh"},
        simulation_ok=True, risk_score=0.3,
        idempotency_key="e2e|token|block",
    )
    base.update(overrides)
    return base
