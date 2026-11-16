"""Run manually inside union-SF after applying sql/20261116_sf_order_node_history.sql."""
import asyncio
import json
import sys
from pathlib import Path

from app.db.pool import get_pool
from app.services.sf_node_history import CODES, parse_time


async def main(path):
    events = set()
    bills = set()
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        bill = row["bill_no"]
        bills.add(bill)
        for step in row.get("steps", []):
            when = parse_time(step.get("time"))
            if step.get("status") in CODES and when is not None:
                events.add((bill, step["status"], when, "sf_wms", ""))
    pool = await get_pool()
    try:
        async with pool.acquire(timeout=10) as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL lock_timeout='5s'")
                await conn.execute("SET LOCAL statement_timeout='120s'")
                await conn.execute(
                    """CREATE TEMP TABLE sf_node_import (
                       bill_no text,node_code text,occurred_at timestamptz,
                       source text,reference text) ON COMMIT DROP"""
                )
                await conn.copy_records_to_table("sf_node_import", records=list(events))
                inserted = await conn.execute(
                    """INSERT INTO public.sf_order_node_event
                       (bill_no,node_code,occurred_at,source,reference)
                       SELECT * FROM sf_node_import ON CONFLICT DO NOTHING"""
                )
                await conn.execute(
                    """INSERT INTO public.sf_order_node_poll (bill_no,checked_at,complete)
                       SELECT bill_no,now(),count(DISTINCT node_code)=3
                       FROM public.sf_order_node_event WHERE bill_no=ANY($1::text[])
                         AND node_code IN ('1700','2400','2900') GROUP BY bill_no
                       ON CONFLICT(bill_no) DO UPDATE SET complete=EXCLUDED.complete,
                       checked_at=EXCLUDED.checked_at""", list(bills),
                )
                missing = await conn.fetchval(
                    """SELECT count(*) FROM (
                       SELECT * FROM sf_node_import EXCEPT
                       SELECT bill_no,node_code,occurred_at,source,reference
                       FROM public.sf_order_node_event) x"""
                )
                if missing:
                    raise RuntimeError(f"Import verification failed: {missing}")
                print(json.dumps({"orders":len(bills),"source_events":len(events),
                                  "insert_result":inserted,"missing":missing}))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))

