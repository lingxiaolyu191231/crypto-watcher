# services/engine/gas.py
from __future__ import annotations
from decimal import Decimal

WEI_PER_ETH = Decimal(10) ** 18
WEI_PER_GWEI = Decimal(10) ** 9

def wei_to_eth(wei: int | Decimal) -> Decimal:
    """Convert wei -> ETH as Decimal."""
    return Decimal(int(wei)) / WEI_PER_ETH

def eth_to_wei(eth: float | int | Decimal) -> int:
    """Convert ETH -> wei (int)."""
    return int(Decimal(str(eth)) * WEI_PER_ETH)

def wei_to_gwei(wei: int | Decimal) -> Decimal:
    """Convert wei -> gwei as Decimal."""
    return Decimal(int(wei)) / WEI_PER_GWEI

def gwei_to_wei(gwei: float | int | Decimal) -> int:
    """Convert gwei -> wei (int)."""
    return int(Decimal(str(gwei)) * WEI_PER_GWEI)

__all__ = ["wei_to_eth", "eth_to_wei", "wei_to_gwei", "gwei_to_wei"]
