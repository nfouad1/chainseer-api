from __future__ import annotations

from chainseer_executable_shadow_v2 import V2ExecutableShadowV2, WETH_ADDRESS


def _word(value: int) -> str:
    return f"{value:064x}"


class FakeRpc:
    def get_block_number(self): return 110
    def get_logs(self, start, end, *, address=None, topics=None):
        return [{"blockNumber":"0x65", "logIndex":"0x2", "transactionHash":"0xabc"}]
    def call(self, address, data, block=None):
        selector=data.removeprefix("0x")
        if selector == "0dfe1681": return "0x" + "0"*24 + WETH_ADDRESS.removeprefix("0x")
        if selector == "d21220a7": return "0x" + "0"*24 + "11"*20
        if selector == "0902f1ac": return "0x" + _word(10**20) + _word(10**24) + _word(0)
        raise AssertionError(selector)


def test_first_sync_selects_entry_then_schedules_outcomes(tmp_path):
    worker=V2ExecutableShadowV2(tmp_path,rpc=FakeRpc())
    worker.store.arm(0.0)
    assert worker.store.append_observation({"envelope_id":"e","source_version":"uniswap_v2","currency0":WETH_ADDRESS,"currency1":"0x"+"11"*20,"pool_address":"0x"+"22"*20,"token_address":"0x"+"11"*20,"block_number":100,"log_index":1,"observed_head":100,"created_at":0.0})
    result=worker.run_once(head_block=110)
    assert result["selected_entries"] == 1
    assert result["resolved_marks"] == 1
    assert worker.store.due_outcomes(20_000,8)[0]["label"] == "15m"


def test_missing_sync_expires_without_relaxing_freshness(tmp_path):
    class NoSync(FakeRpc):
        def get_logs(self, *args, **kwargs): return []
    worker=V2ExecutableShadowV2(tmp_path,rpc=NoSync())
    worker.store.arm(0.0)
    assert worker.store.append_observation({"envelope_id":"e","source_version":"uniswap_v2","currency0":WETH_ADDRESS,"currency1":"0x"+"11"*20,"pool_address":"0x"+"22"*20,"token_address":"0x"+"11"*20,"block_number":100,"log_index":1,"observed_head":100,"created_at":0.0})
    result=worker.run_once(head_block=221)
    assert result["expired_without_liquidity"] == 1
    assert result["selected_entries"] == 0
