from __future__ import annotations

from chainseer_executable_shadow import (
    ExecutableShadowStore, V2ExecutableShadow, WETH_ADDRESS,
)


def _word(value: int) -> str:
    return f"{value:064x}"


class FakeRpc:
    def call(self, address, data, block=None):
        selector = data.removeprefix("0x")
        if selector == "0dfe1681":
            return "0x" + "0" * 24 + WETH_ADDRESS.removeprefix("0x")
        if selector == "d21220a7":
            return "0x" + "0" * 24 + "11" * 20
        if selector == "0902f1ac":
            return "0x" + _word(10**20) + _word(10**24) + _word(0)
        raise AssertionError((address, data, block))


def _envelope():
    return {
        "envelope_id": "prospective-envelope", "source_version": "uniswap_v2",
        "currency0": WETH_ADDRESS, "currency1": "0x" + "11" * 20,
        "pool_address": "0x" + "22" * 20, "token_address": "0x" + "11" * 20,
        "block_number": 100, "observed_head": 102, "created_at": 1.0,
    }


def test_v2_shadow_records_immutable_entry_and_later_exit(tmp_path):
    worker = V2ExecutableShadow(tmp_path, rpc=FakeRpc())
    worker.store.arm(0.0)
    assert worker.store.append_observation(_envelope())
    entry = worker.store.due_marks(1_000_000)[0]
    assert entry["label"] == "entry"
    worker.store.append_mark(entry, quote=worker._quote(entry), now=2.0)
    due = worker.store.due_marks(1_000_000)
    assert {row["label"] for row in due} == {"15m", "1h", "6h", "24h"}
    outcome = next(row for row in due if row["label"] == "15m")
    quote = worker._quote(outcome)
    assert quote["verified"] and quote["exitable"]
    worker.store.append_mark(outcome, quote=quote, now=3.0)
    status = worker.store.snapshot()
    assert status["resolved_marks"] == 2
    assert status["shadow_only"] is True
    assert status["paper_execution_enabled"] is False
    assert status["live_execution_enabled"] is False
