from __future__ import annotations
import argparse
import asyncio
import os
import json
import time
from decimal import Decimal
from itertools import product

from loguru import logger
from dotenv import load_dotenv
import yaml

from services.ingestors.zeroex import ZeroEx
from services.ingestors.aerodrome_router import AerodromeRouter
from services.engine.pricing import to_wei, from_wei
from services.engine.gas import wei_to_eth
from services.alerts import email as email_alert
from services.storage.postgres import init_schema, get_conn

load_dotenv()

# ---- Thresholds / buffers (env-configurable) ----
DEF_MEV_BPS = Decimal(os.getenv("MEV_BUFFER_BPS", "5"))    # e.g., 5 bps haircut for MEV risk
MIN_PROFIT_USD = Decimal(os.getenv("MIN_PROFIT_USD", "1")) # minimum absolute profit in USD to alert
MIN_ROI_BPS = Decimal(os.getenv("MIN_ROI_BPS", "5"))       # minimum net bps to alert
ETH_USD = Decimal(os.getenv("ETH_USD", "0"))               # set >0 to price gas in USD; leave 0 to ignore gas
ALERT_COOLDOWN_S = int(os.getenv("ALERT_COOLDOWN_S", "60"))

# Assume base stable ~ $1 (USDC/USDT). If you broaden tokens, pull per-token USD.
USD_PER_BASE = Decimal("1.0")

# In-memory cooldown registry to suppress duplicate alerts
_LAST_ALERT: dict[str, float] = {}


def _alert_key(pair: str, size: Decimal, leg_a: str, leg_b: str) -> str:
    return f"{pair}|{size}|{leg_a}->{leg_b}"


async def _try_leg(provider, sell_addr: str, buy_addr: str, amount_wei: int):
    """
    Uniform adapter call for any provider with .quote(sell, buy, amount_wei) -> {buy_amount, gas?, gas_price?}
    """
    try:
        return await provider.quote(sell_addr, buy_addr, amount_wei)
    except Exception as e:
        name = getattr(provider, "included_sources", provider.__class__.__name__)
        logger.debug(f"quote error from {name}: {e}")
        return None


async def evaluate_and_alert(
    *,
    chain_id: int,
    base_sym: str,
    quote_sym: str,
    base_decimals: int,
    size_units: Decimal,
    qa: dict,
    qb: dict,
    leg_a: str,
    leg_b: str,
    opps_out: list,
):
    """
    Compute gross/net pnl, apply thresholds, send email, and stage DB record.
    """
    pair = f"{base_sym}/{quote_sym}"

    # PnL in base token units
    out_back_wei = int(qb["buy_amount"])
    sell_amount_wei = to_wei(size_units, base_decimals)
    profit_wei = out_back_wei - sell_amount_wei
    profit_base = from_wei(profit_wei, base_decimals)

    # Gas (optional USD pricing if ETH_USD>0)
    gas_units = int(qa.get("gas", 250_000)) + int(qb.get("gas", 250_000))
    gas_price_wei = int(qa.get("gas_price", 0) or qb.get("gas_price", 0) or 0)
    gas_eth = wei_to_eth(gas_units * gas_price_wei) if gas_price_wei else Decimal(0)
    gas_usd = gas_eth * ETH_USD if ETH_USD > 0 else Decimal(0)

    # BPS metrics
    gross_bps = (profit_base / size_units) * Decimal(10_000) if size_units > 0 else Decimal(0)
    mev_cut = size_units * (DEF_MEV_BPS / Decimal(10_000))
    net_base_mev = profit_base - mev_cut
    roi_mev_bps = (net_base_mev / size_units) * Decimal(10_000) if size_units > 0 else Decimal(0)

    # Convert to net USD after gas
    net_usd = (net_base_mev * USD_PER_BASE) - gas_usd
    roi_net_bps = (net_usd / (size_units * USD_PER_BASE)) * Decimal(10_000) if size_units > 0 else Decimal(0)

    # Threshold gate
    if net_usd < MIN_PROFIT_USD or roi_net_bps < MIN_ROI_BPS:
        return

    # Duplicate-alert cooldown
    key = _alert_key(pair, size_units, leg_a, leg_b)
    now = time.time()
    if now - _LAST_ALERT.get(key, 0) < ALERT_COOLDOWN_S:
        return
    _LAST_ALERT[key] = now

    # Alert
    msg = (
        f"[ARB] {pair} size {size_units} on chain {chain_id}\n"
        f"A: {leg_a} -> buy {quote_sym} | B: {leg_b} -> sell back\n"
        f"Gross: {profit_base:.4f} {base_sym} | Gross bps: {gross_bps:.1f}\n"
        f"MEV buffer: {DEF_MEV_BPS} bps | ROI (MEV-only): {roi_mev_bps:.1f} bps\n"
        f"Gas(est): ${gas_usd:.2f} (ETH_USD={ETH_USD}) | ROI (net): {roi_net_bps:.1f} bps\n"
        f"Net: ${net_usd:.2f}"
    )
    logger.success(msg)
    await email_alert.send(subject=f"ARB {pair} {size_units} {leg_a}->{leg_b}", body=msg)

    opps_out.append(
        {
            "pair": pair,
            "size": float(size_units),
            "chain_id": chain_id,
            "gross_base": float(profit_base),
            "gross_bps": float(gross_bps),
            "net_usd": float(net_usd),
            "gas_usd": float(gas_usd),
            "details": {
                "leg_a": leg_a,
                "leg_b": leg_b,
                "qa": qa,
                "qb": qb,
                "roi_mev_bps": float(roi_mev_bps),
                "roi_net_bps": float(roi_net_bps),
            },
        }
    )


async def scan_once(cfg_tokens: dict, cfg_pairs: list[list[str]], sizes: list, source_list: list[str]):
    """
    Main scan pass:
      1) 0x -> 0x (A != B) across selected sources (e.g., Uniswap_V3 | Balancer_V2 | SushiSwap)
      2) 0x -> Aerodrome (direct)
      3) Aerodrome -> 0x
    """
    chain_id = cfg_tokens.get("chain_id", 8453)
    tokens = cfg_tokens["symbols"]

    # 0x providers pinned to single source per leg
    ox = {src: ZeroEx(included_sources=src) for src in source_list}
    # Direct Aerodrome router (Solidly/Velodrome style)
    aero = AerodromeRouter()

    opps: list[dict] = []

    for base_sym, quote_sym in cfg_pairs:
        base = tokens[base_sym]
        quote = tokens[quote_sym]
        base_dec = int(base["decimals"])

        for size in sizes:
            size_units = Decimal(size)
            sell_amount_wei = to_wei(size_units, base_dec)

            # (1) 0x -> 0x (ordered pairs)
            for src_a, src_b in product(source_list, source_list):
                if src_a == src_b:
                    continue
                qa = await _try_leg(ox[src_a], base["address"], quote["address"], sell_amount_wei)
                if not qa or qa["buy_amount"] == 0:
                    continue
                qb = await _try_leg(ox[src_b], quote["address"], base["address"], qa["buy_amount"])
                if not qb or qb["buy_amount"] == 0:
                    continue

                await evaluate_and_alert(
                    chain_id=chain_id,
                    base_sym=base_sym,
                    quote_sym=quote_sym,
                    base_decimals=base_dec,
                    size_units=size_units,
                    qa=qa,
                    qb=qb,
                    leg_a=src_a,
                    leg_b=src_b,
                    opps_out=opps,
                )

            # (2) 0x -> Aerodrome
            for src_a in source_list:
                qa = await _try_leg(ox[src_a], base["address"], quote["address"], sell_amount_wei)
                if not qa or qa["buy_amount"] == 0:
                    continue
                qb = await _try_leg(aero, quote["address"], base["address"], qa["buy_amount"])
                if not qb or qb["buy_amount"] == 0:
                    continue

                await evaluate_and_alert(
                    chain_id=chain_id,
                    base_sym=base_sym,
                    quote_sym=quote_sym,
                    base_decimals=base_dec,
                    size_units=size_units,
                    qa=qa,
                    qb=qb,
                    leg_a=src_a,
                    leg_b="Aerodrome_V1",
                    opps_out=opps,
                )

            # (3) Aerodrome -> 0x
            qa = await _try_leg(aero, base["address"], quote["address"], sell_amount_wei)
            if qa and qa["buy_amount"] > 0:
                for src_b in source_list:
                    qb = await _try_leg(ox[src_b], quote["address"], base["address"], qa["buy_amount"])
                    if not qb or qb["buy_amount"] == 0:
                        continue

                    await evaluate_and_alert(
                        chain_id=chain_id,
                        base_sym=base_sym,
                        quote_sym=quote_sym,
                        base_decimals=base_dec,
                        size_units=size_units,
                        qa=qa,
                        qb=qb,
                        leg_a="Aerodrome_V1",
                        leg_b=src_b,
                        opps_out=opps,
                    )

    # Optional: persist to Postgres
    if opps:
        with get_conn() as conn:
            if conn:
                cur = conn.cursor()
                for o in opps:
                    cur.execute(
                        """
                        INSERT INTO opportunities(
                            chain_id, base_symbol, quote_symbol, size, dex_a, dex_b,
                            gross_bps, net_usd, gas_usd, details
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            o["chain_id"],
                            o["pair"].split("/")[0],
                            o["pair"].split("/")[1],
                            o["size"],
                            o["details"]["leg_a"],
                            o["details"]["leg_b"],
                            o["gross_bps"],
                            o["net_usd"],
                            o["gas_usd"],
                            json.dumps(o["details"]),
                        ),
                    )
                conn.commit()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--network", default=os.getenv("NETWORK", "base"))
    parser.add_argument("--tokens", default="configs/tokens.base.yml")
    parser.add_argument("--pairs", default="configs/pairs.base.yml")
    args = parser.parse_args()

    init_schema()

    with open(args.tokens, "r") as f:
        cfg_tokens = yaml.safe_load(f)
    with open(args.pairs, "r") as f:
        cfg_pairs = yaml.safe_load(f)

    pairs = cfg_pairs["pairs"]
    sizes = cfg_pairs["sizes"]
    source_list = cfg_pairs.get("sources", ["Uniswap_V3", "Balancer_V2", "SushiSwap"])

    logger.info("Starting arb observer (0x + Aerodrome legs)...")
    while True:
        try:
            await scan_once(cfg_tokens, pairs, sizes, source_list)
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(1.0)


if __name__ == "__main__":
    asyncio.run(main())


