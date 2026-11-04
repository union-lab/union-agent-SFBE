"""Per-order gates. No global pause, expiry, auto-release, or DDL.

The lock is shared with the existing Kingdee outstock creator/audit retry.
Holds commit independently: an already dispatched request may finish, but the
acknowledgement reports that uncertainty and every subsequent dispatch is gated.
"""

import hashlib
import json
from contextlib import asynccontextmanager
from xml.etree import ElementTree as ET

from app.db.pool import get_pool


def lock_key(bill_no: str) -> int:
    return int.from_bytes(
        hashlib.sha256(f"sf_outstock:{bill_no}".encode()).digest()[:8],
        "big", signed=True,
    )


def version(rec) -> str:
    if not rec:
        return "NOT_PUSHED"
    # Exclude polling timestamps: only changes in business evidence invalidate a view.
    keys = ("id", "bill_no", "erp_order", "sf_receipt_id", "status",
            "outstock_no", "cancelled_at", "sf_wms_status")
    data = {key: str(rec.get(key) or "") for key in keys}
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


async def held(conn, bill_no: str) -> bool:
    return bool(await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM sf_api_hold WHERE bill_no=$1)", bill_no,
    ))


@asynccontextmanager
async def outbound_gate(content: str, *, cancellation: bool = False):
    """Only exact outbound order IDs; malformed/multi-order payloads fail closed."""
    orders = ET.fromstring(content).findall(".//SaleOrders/SaleOrder")
    bills = [order.findtext("ErpOrder", "").strip() for order in orders]
    if not bills or any(not b for b in bills):
        yield False
        return
    pool = await get_pool()
    async with pool.acquire() as conn, conn.transaction():
        # Try locks, never let one busy/refunded order stall the scan behind it.
        for bill in sorted(set(bills)):
            if not await conn.fetchval(
                "SELECT pg_try_advisory_xact_lock($1)", lock_key(bill),
            ) or await held(conn, bill):
                yield False
                return
        if cancellation:
            # Commit before external IO on another connection. A concurrent
            # newly-created hold must not cause the worker to repeat this call.
            async with pool.acquire() as journal:
                for bill in set(bills):
                    await journal.execute(
                        """INSERT INTO sf_api_legacy_cancel(bill_no) VALUES($1)
                           ON CONFLICT(bill_no) DO UPDATE SET attempted_at=now()""",
                        bill,
                    )
        # Hold the per-order lock through network IO; a hold is still durable if
        # it arrives while this request is in flight (and reports IN_FLIGHT).
        yield True


async def heartbeat():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO sf_api_runtime(singleton,protocol,heartbeat_at)
               VALUES (true,1,now()) ON CONFLICT(singleton) DO UPDATE
               SET protocol=1,heartbeat_at=now()"""
        )
