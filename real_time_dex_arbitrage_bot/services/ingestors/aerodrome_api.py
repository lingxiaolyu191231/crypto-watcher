# services/ingestors/aerodrome_api.py
from __future__ import annotations
import os, httpx
from decimal import Decimal

AERODROME_API_BASE = os.getenv("AERODROME_API_BASE", "https://aerodrome-api.quicknode.com")
AERODROME_API_KEY  = os.getenv("AERODROME_API_KEY")  # Bearer

def _to_units(amount_wei: int, decimals: int) -> str:
    # amount as token units (string), e.g., 1.23
    q = Decimal(amount_wei) / (Decimal(10) ** decimals)
    # API accepts number; keep a compact string
    return format(q, "f")

async def quote_aerodrome_api(
    sell_addr: str,
    buy_addr: str,
    amount_wei: int,
    sell_decimals: int,
    slippage: float = 0.005,
    target: str = "base",
):
    """
    Returns dict with:
      - buy_amount: int (amountOut in wei)
      - protocol: "Aerodrome_API"
      - meta: raw JSON (optional for debugging)
    """
    if not AERODROME_API_BASE:
        return None

    url = f"{AERODROME_API_BASE}/v1/quote"
    params = {
        "target": target,
        "from_token": sell_addr,
        "to_token": buy_addr,
        "amount": _to_units(amount_wei, sell_decimals),  # token units, not wei
        "slippage": str(slippage),
    }
    headers = {"accept": "application/json"}
    if AERODROME_API_KEY:
        headers["Authorization"] = f"Bearer {AERODROME_API_KEY}"

    async with httpx.AsyncClient(timeout=3.5) as client:
        r = await client.get(url, params=params, headers=headers)
        if r.status_code != 200:
            return None
        q = r.json()

        # expected shape per docs/guides:
        # q["output"]["amount_wei"] (string) or q["output"]["amount"] (float units)
        out_wei = int(q.get("output", {}).get("amount_wei", "0"))
        if out_wei <= 0:
            return None

        return {
            "buy_amount": out_wei,
            "gas": int(q.get("gas", 0)) if isinstance(q.get("gas", 0), int) else 250000,
            "gas_price": int(q.get("gasPrice", 0)) if isinstance(q.get("gasPrice", 0), int) else 0,
            "protocol": "Aerodrome_API",
            "meta": q,
        }
