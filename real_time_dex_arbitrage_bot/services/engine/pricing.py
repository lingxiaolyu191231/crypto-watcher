# services/engine/pricing.py
from __future__ import annotations
from decimal import Decimal, ROUND_DOWN, getcontext

# high precision for token math
getcontext().prec = 60

def _dec(x) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))

def to_wei(amount, decimals: int) -> int:
    """
    Convert token units -> integer base units (wei-style).
    Rounds DOWN to avoid overestimating buy/sell size.
    """
    amt = _dec(amount)
    scale = Decimal(10) ** int(decimals)
    return int((amt * scale).to_integral_value(rounding=ROUND_DOWN))

def from_wei(amount_wei: int, decimals: int) -> Decimal:
    """
    Convert integer base units (wei-style) -> token units (Decimal).
    """
    scale = Decimal(10) ** int(decimals)
    return Decimal(int(amount_wei)) / scale

__all__ = ["to_wei", "from_wei"]
