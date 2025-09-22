# services/ingestors/aerodrome_router.py (hardened)
from __future__ import annotations
import asyncio, os
from typing import Optional
from web3 import Web3

# Env-configurable; keep your Base defaults in .env
AERO_ROUTER_ENV  = os.getenv("AERODROME_ROUTER", "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43")
AERO_FACTORY_ENV = os.getenv("AERODROME_FACTORY", "0x420DD381b31aEf6683db6B902084cB0FFECe40Da")
RPC_URL          = os.getenv("RPC_URL")

# ABI with factory in route tuple (Velodrome-style)
ROUTER_ABI_4 = [{
    "name": "getAmountsOut", "type": "function", "stateMutability": "view",
    "inputs": [
        {"name": "amountIn", "type": "uint256"},
        {"name": "routes", "type": "tuple[]", "components": [
            {"name": "from", "type": "address"},
            {"name": "to", "type": "address"},
            {"name": "stable", "type": "bool"},
            {"name": "factory", "type": "address"},
        ]},
    ],
    "outputs": [{"name": "amounts", "type": "uint256[]"}],
}]

# ABI without factory field (some deployments)
ROUTER_ABI_3 = [{
    "name": "getAmountsOut", "type": "function", "stateMutability": "view",
    "inputs": [
        {"name": "amountIn", "type": "uint256"},
        {"name": "routes", "type": "tuple[]", "components": [
            {"name": "from", "type": "address"},
            {"name": "to", "type": "address"},
            {"name": "stable", "type": "bool"},
        ]},
    ],
    "outputs": [{"name": "amounts", "type": "uint256[]"}],
}]

class AerodromeRouter:
    def __init__(self, rpc_url: Optional[str] = None, router: Optional[str] = None, factory: Optional[str] = None):
        if not (rpc_url or RPC_URL):
            raise RuntimeError("RPC_URL is required for AerodromeRouter")
        self.web3 = Web3(Web3.HTTPProvider(rpc_url or RPC_URL))

        # Ensure checksum addresses
        self.router_addr  = Web3.to_checksum_address(router  or AERO_ROUTER_ENV)
        self.factory_addr = Web3.to_checksum_address(factory or AERO_FACTORY_ENV)

        # Prebuild both contract variants
        self.router4 = self.web3.eth.contract(address=self.router_addr, abi=ROUTER_ABI_4)
        self.router3 = self.web3.eth.contract(address=self.router_addr, abi=ROUTER_ABI_3)

    async def _call_get_amounts_out(self, amount_in_wei: int, route, use_factory: bool):
        loop = asyncio.get_running_loop()
        if use_factory:
            return await loop.run_in_executor(None, lambda: self.router4.functions.getAmountsOut(amount_in_wei, route).call())
        else:
            return await loop.run_in_executor(None, lambda: self.router3.functions.getAmountsOut(amount_in_wei, route).call())

    async def quote(self, sell_addr: str, buy_addr: str, amount_in_wei: int, stable: bool = True):
        # Always pass checksum token addresses
        t_in  = Web3.to_checksum_address(sell_addr)
        t_out = Web3.to_checksum_address(buy_addr)

        # Try (from,to,stable,factory) first; if it fails, try (from,to,stable)
        attempts = [
            (True,  (t_in,  t_out, stable, self.factory_addr)),
            (False, (t_in,  t_out, stable)),
        ]

        # Also allow auto-fallback stable -> not stable
        for use_factory, base_route in attempts:
            for st in (stable, (not stable)):
                route = [tuple(base_route[:2]) + (st,) + (() if not use_factory else (self.factory_addr,))]
                try:
                    amounts = await self._call_get_amounts_out(amount_in_wei, route, use_factory)
                    if amounts and len(amounts) >= 2:
                        return {
                            "buy_amount": int(amounts[-1]),
                            "protocol": f"Aerodrome_V1_{'4f' if use_factory else '3f'}",
                            "stable": st,
                        }
                except Exception:
                    continue
        return None

