# services/ingestors/uniswapv3.py (hardened: V2-first with V1 fallback)
from __future__ import annotations
import asyncio, os
from typing import Optional
from web3 import Web3

RPC_URL = os.getenv("RPC_URL")
QUOTER_ADDR_ENV = os.getenv("UNIV3_QUOTER_ADDRESS")  # set this to your QuoterV2 address on Base

# Quoter V2 ABI (tuple param, multiple outputs, nonpayable)
QUOTER_V2_ABI = [{
    "name": "quoteExactInputSingle",
    "type": "function",
    "stateMutability": "nonpayable",
    "inputs": [{
        "name": "params", "type": "tuple", "components": [
            {"name": "tokenIn", "type": "address"},
            {"name": "tokenOut", "type": "address"},
            {"name": "amountIn", "type": "uint256"},
            {"name": "fee", "type": "uint24"},
            {"name": "sqrtPriceLimitX96", "type": "uint160"},
        ]
    }],
    "outputs": [
        {"name": "amountOut", "type": "uint256"},
        {"name": "sqrtPriceX96After", "type": "uint160"},
        {"name": "initializedTicksCrossed", "type": "uint32"},
        {"name": "gasEstimate", "type": "uint256"},
    ],
}]

# Quoter V1 ABI (positional args, single output, view)
QUOTER_V1_ABI = [{
    "name": "quoteExactInputSingle",
    "type": "function",
    "stateMutability": "view",
    "inputs": [
        {"name": "tokenIn", "type": "address"},
        {"name": "tokenOut", "type": "address"},
        {"name": "fee", "type": "uint24"},
        {"name": "amountIn", "type": "uint256"},
        {"name": "sqrtPriceLimitX96", "type": "uint160"},
    ],
    "outputs": [{"name": "amountOut", "type": "uint256"}],
}]


class UniswapV3:
    def __init__(self, rpc_url: Optional[str] = None, quoter_address: Optional[str] = None):
        rpc = rpc_url or RPC_URL
        if not rpc:
            raise RuntimeError("RPC_URL not set (needed for UniswapV3 quoter).")
        qaddr = quoter_address or QUOTER_ADDR_ENV
        if not qaddr:
            raise RuntimeError("UNIV3_QUOTER_ADDRESS not set.")

        self.web3 = Web3(Web3.HTTPProvider(rpc))
        self.addr = Web3.to_checksum_address(qaddr)
        self.qv2 = self.web3.eth.contract(address=self.addr, abi=QUOTER_V2_ABI)
        self.qv1 = self.web3.eth.contract(address=self.addr, abi=QUOTER_V1_ABI)

    async def quote(self, token_in: str, token_out: str, fee: int, amount_in: int, sqrt_price_limit_x96: int = 0):
        """Return {'buy_amount': int, 'protocol': 'uniswapv3_v2'|'uniswapv3_v1'} or None."""
        loop = asyncio.get_running_loop()
        t_in = Web3.to_checksum_address(token_in)
        t_out = Web3.to_checksum_address(token_out)

        # Try V2 first
        params = (t_in, t_out, int(amount_in), int(fee), int(sqrt_price_limit_x96))
        try:
            # V2 expects a single tuple argument
            v2_res = await loop.run_in_executor(None, lambda: self.qv2.functions.quoteExactInputSingle(params).call())
            # v2_res is a tuple; first element is amountOut
            amount_out = int(v2_res[0]) if isinstance(v2_res, (list, tuple)) else int(v2_res)
            if amount_out > 0:
                return {"buy_amount": amount_out, "protocol": "uniswapv3_v2"}
        except Exception:
            pass

        # Fallback to V1 (positional args)
        try:
            v1_res = await loop.run_in_executor(
                None,
                lambda: self.qv1.functions.quoteExactInputSingle(t_in, t_out, int(fee), int(amount_in), int(sqrt_price_limit_x96)).call()
            )
            amount_out = int(v1_res)
            if amount_out > 0:
                return {"buy_amount": amount_out, "protocol": "uniswapv3_v1"}
        except Exception:
            pass

        return None
