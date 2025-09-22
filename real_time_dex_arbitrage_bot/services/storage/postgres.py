# services/storage/postgres.py (psycopg 3)
from contextlib import contextmanager
import os, json
import psycopg

DSN = os.getenv("PG_DSN")

@contextmanager
def get_conn():
    if not DSN:
        yield None
        return
    with psycopg.connect(DSN) as conn:
        yield conn

def init_schema():
    if not DSN:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS opportunities (
                  id BIGSERIAL PRIMARY KEY,
                  ts TIMESTAMPTZ DEFAULT now(),
                  chain_id INT,
                  base_symbol TEXT,
                  quote_symbol TEXT,
                  size DECIMAL,
                  dex_a TEXT,
                  dex_b TEXT,
                  gross_bps DECIMAL,
                  net_usd DECIMAL,
                  gas_usd DECIMAL,
                  details JSONB
                );
                CREATE INDEX IF NOT EXISTS opportunities_ts_idx ON opportunities (ts DESC);
            """)
            conn.commit()

