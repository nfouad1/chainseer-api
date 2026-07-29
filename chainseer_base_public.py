"""First-class public Base analyzer backed by Chainseer's production EVM core."""

from __future__ import annotations

from chainseer import BASE_NETWORK, Chainseer


class BasePublicAnalyzer(Chainseer):
    """Base Mainnet analysis with isolated endpoints and shared Timechain."""

    def __init__(
        self,
        rpc_url: str | None = None,
        *,
        timechain_agent=None,
        chain_root: str | None = None,
        cross_chain_provider=None,
        social_kol_provider=None,
    ):
        super().__init__(
            rpc_url=rpc_url or BASE_NETWORK.rpc_url,
            chain_root=chain_root,
            cross_chain_provider=cross_chain_provider,
            social_kol_provider=social_kol_provider,
            network=BASE_NETWORK,
            timechain_agent=timechain_agent,
        )

