"""Durable SF milestones. No runtime DDL; events are append-only and idempotent."""
import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape

from app.db.pool import get_pool

logger = logging.getLogger("union.sf_history")
CN = timezone(timedelta(hours=8))
CODES = {"1700", "2400", "2900"}


def parse_time(value):
    if not value:
        return None
    try:
        result = datetime.fromisoformat(str(value).strip())
        return result.replace(tzinfo=CN) if result.tzinfo is None else result
    except ValueError:
        return None


def extract_events(raw_xml, expected):
    """Do not mix orders or use header timestamps as substitutes for steps."""
    root = ET.fromstring(raw_xml)
    events = set()
    returned = set()
    for order in root.findall(".//SaleOrders/SaleOrder"):
        bill = order.findtext("ErpOrder") or order.findtext("Header/ErpOrder")
        if bill not in expected or order.findtext("Result") != "1":
            continue
        returned.add(bill)
        for step in order.findall("Steps/Step"):
            code = step.findtext("Status")
            when = parse_time(step.findtext("EventTime"))
            if code in CODES and when is not None:
                events.add((bill, code, when, "sf_wms", ""))
    return sorted(events), returned


async def insert_events(conn, events):
    if events:
        await conn.executemany(
            """INSERT INTO public.sf_order_node_event
               (bill_no,node_code,occurred_at,source,reference)
               VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING""", events,
        )


async def store_response(raw_xml, expected):
    events, returned = extract_events(raw_xml, set(expected))
    pool = await get_pool()
    async with pool.acquire(timeout=5) as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            await insert_events(conn, events)
            for bill in returned:
                await conn.execute(
                    """INSERT INTO public.sf_order_node_poll (bill_no,checked_at,complete)
                       VALUES ($1,now(),(SELECT count(DISTINCT node_code)=3
                         FROM public.sf_order_node_event
                         WHERE bill_no=$1 AND node_code IN ('1700','2400','2900')))
                       ON CONFLICT (bill_no) DO UPDATE SET checked_at=now(),
                       complete=EXCLUDED.complete, attempts=sf_order_node_poll.attempts+1""", bill,
                )
    return len(events)


async def safe_store_response(raw_xml, expected):
    try:
        return await asyncio.wait_for(store_response(raw_xml, expected), timeout=12)
    except Exception:
        logger.exception("SF node history persistence failed for %s", expected)
        return 0


async def record_waybill_saved(outstock_no, sale_order_no, waybill_no, occurred_at):
    """Record only an actual successful Save; an already-present value has unknown age."""
    try:
        pool = await get_pool()
        async with pool.acquire(timeout=5) as conn:
            await conn.execute(
                """INSERT INTO public.sf_order_node_event
                   (bill_no,node_code,occurred_at,source,reference)
                   SELECT bill_no,'kingdee_waybill_saved',$2,'kingdee_sale_order',$3
                   FROM ads_sf_push_record WHERE outstock_no=$1
                   ON CONFLICT DO NOTHING""",
                outstock_no, occurred_at, sale_order_no + ":" + waybill_no, timeout=5,
            )
    except Exception:
        logger.exception("Waybill save event persistence failed: %s", outstock_no)


async def sync_missing_nodes(limit=100):
    """Two bounded requests per run; never query Kingdee or touch business status."""
    from app.config import settings
    from app.services.sf_outbound import _build_xml, _send_request

    pool = await get_pool()
    async with pool.acquire(timeout=5) as conn:
        rows = await conn.fetch(
            """SELECT r.bill_no FROM ads_sf_push_record r
               LEFT JOIN public.sf_order_node_poll p ON p.bill_no=r.bill_no
               WHERE r.bill_type IN ('outbound','manual_outbound','transfer_out','transfer_in_out')
                 AND r.created_at >= '2026-08-16 00:00:00+08'::timestamptz
                 AND r.status NOT LIKE 'cancelled%'
                 AND COALESCE(r.sf_wms_status,'') <> '1400'
                 AND NOT COALESCE(p.complete,false)
                 AND (p.checked_at IS NULL OR p.checked_at < now()-interval '30 minutes')
               ORDER BY p.checked_at ASC NULLS FIRST,r.created_at,r.id LIMIT $1""",
            min(max(limit, 1), 100), timeout=8,
        )
        # Claim before networking; failed requests also yield fairly to other orders.
        await conn.executemany(
            """INSERT INTO public.sf_order_node_poll(bill_no,checked_at) VALUES($1,now())
               ON CONFLICT(bill_no) DO UPDATE SET checked_at=now(),
               attempts=sf_order_node_poll.attempts+1""",
            [(r["bill_no"],) for r in rows],
        )
    for start in range(0, len(rows), 50):
        bills = [r["bill_no"] for r in rows[start:start + 50]]
        body = (
            "<SaleOrderStatusQueryRequest><CompanyCode>" + escape(settings.sf_company_code)
            + "</CompanyCode><SaleOrders>"
            + "".join("<SaleOrder><ErpOrder>" + escape(b) + "</ErpOrder></SaleOrder>" for b in bills)
            + "</SaleOrders></SaleOrderStatusQueryRequest>"
        )
        response = await _send_request("SALE_ORDER_STATUS_QUERY_SERVICE", _build_xml("SALE_ORDER_STATUS_QUERY_SERVICE", body))
        if response.get("head") == "OK":
            await store_response(response.get("raw", ""), bills)
        await asyncio.sleep(1)
    return len(rows)

