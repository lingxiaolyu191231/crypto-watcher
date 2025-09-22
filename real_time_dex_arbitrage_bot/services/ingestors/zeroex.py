from __future__ import annotations
import os
from typing import Optional, Union, List
import httpx


class ZeroEx:
    """
    Thin wrapper around 0x Swap API /swap/v1/quote.
    Use included_sources to pin routing to a single DEX (e.g., "Uniswap_V3", "Balancer_V2", "SushiSwap"),
    or pass a list which will be joined by commas per 0x docs.
    """

    def __init__(
        self,
        api_base: Optional[str] = None,
        api_key: Optional[str] = None,
        included_sources: Optional[Union[str, List[str]]] = None,
        timeout_s: float = 3.5,
    ):
        self.base = (api_base or os.getenv("ZEROX_API_BASE", "https://base.api.0x.org")) + "/swap/v1"
        self.key = api_key or os.getenv("ZEROX_API_KEY")
        if isinstance(included_sources, list):
            included_sources = ",".join(included_sources)
        self.included_sources = included_sources  # e.g. "Uniswap_V3" or "Balancer_V2,SushiSwap"
        self.timeout_s = timeout_s

    async def quote(self, sell_token: str, buy_token: str, sell_amount_wei: int):
        url = f"{self.base}/quote"
        params = {
            "sellToken": sell_token,
            "buyToken": buy_token,
            "sellAmount": str(sell_amount_wei),
            "slippagePercentage": "0.001",
            "skipValidation": "true",
        }
        if self.included_sources:
            params["includedSources"] = self.included_sources

        headers = {"Accept": "application/json"}
        if self.key:
            headers["0x-api-key"] = self.key

        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            r = await client.get(url, params=params, headers=headers)
            if r.status_code != 200:
                return None
            q = r.json()
            return {
                "buy_amount": int(q.get("buyAmount", 0)),
                "gas": int(q.get("gas", 250000)),
                "gas_price": int(q.get("gasPrice", 0)),
                "tx_to": q.get("to"),
                "tx_data": q.get("data"),
                "sources": q.get("sources", []),  # which DEXes 0x actually used
                "protocol": "0x",
            }
