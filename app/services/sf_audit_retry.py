"""Delayed audit-only recovery for existing SF outbound documents, without DDL."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone

from app.clients.kingdee import KingdeeClient
from app.config import settings
from app.db.pool import get_pool

logger = logging.getLogger("union.sf_audit_retry")
MARKER = re.compile(r"\[sf-audit-retry:(\d+)\]")
MAX_ATTEMPTS = 8
GSP_ERRORS = ("实发数量不能大于合格数量加不合格可出库数量", "存在还未复核完毕的")


def retry_due(rec, now: datetime) -> bool:
    error = rec["error_message"] or ""
    match = MARKER.search(error)
    attempts = int(match[1]) if match else 0
    return (
        rec["bill_type"] == "outbound"
        and rec["status"] == "outstock_failed"
        and bool(rec["outstock_no"])
        and not rec["cancelled_at"]
        and not rec["unaudit_detected_at"]
        and "GSP" in error
        and any(part in error for part in GSP_ERRORS)
        and attempts < MAX_ATTEMPTS
        and now >= rec["updated_at"] + timedelta(minutes=min(2 ** (attempts + 1), 30))
    )


async def _document(kd, bill_no: str):
    rows = await kd.query(
        "SAL_OUTSTOCK", "FID,FBillNo,FDocumentStatus",
        filter_string=f"FSrcBillNo = '{bill_no}'", limit=501,
    )
    # A joined query can return several lines per document. Never choose among duplicates.
    if len(rows) >= 501 or any(not isinstance(row, list) or len(row) != 3 for row in rows):
        return None
    docs = {tuple(str(value) for value in row) for row in rows}
    return next(iter(docs)) if len(docs) == 1 else None


async def _retry_one(pool, record_id: int) -> bool:
    async with pool.acquire() as conn:
        rec = await conn.fetchrow("SELECT * FROM ads_sf_push_record WHERE id = $1", record_id)
        if not rec or not retry_due(rec, datetime.now(timezone.utc)):
            return False
        bill_no = rec["bill_no"]
        if not re.fullmatch(r"[A-Za-z0-9_-]+", bill_no):
            return False
        key = int.from_bytes(hashlib.sha256(f"sf_outstock:{bill_no}".encode()).digest()[:8], "big", signed=True)
        if not await conn.fetchval("SELECT pg_try_advisory_lock($1)", key):
            return False
        kd = None
        claimed_error = None
        attempt = 0
        try:
            # Re-read after taking the same per-bill lock used by document creation.
            rec = await conn.fetchrow("SELECT * FROM ads_sf_push_record WHERE id = $1", record_id)
            if not rec or not retry_due(rec, datetime.now(timezone.utc)):
                return False
            error = rec["error_message"] or ""
            match = MARKER.search(error)
            attempt = (int(match[1]) if match else 0) + 1
            original = MARKER.sub("", error).split("\n补审结果:", 1)[0].strip()
            claimed_error = f"[sf-audit-retry:{attempt}] {original}"
            # Autocommit BEFORE external writes: crashes/restarts retain attempt and backoff.
            claimed = await conn.fetchval(
                """UPDATE ads_sf_push_record SET error_message=$2, updated_at=now()
                   WHERE id=$1 AND status='outstock_failed' AND cancelled_at IS NULL
                     AND unaudit_detected_at IS NULL RETURNING id""", record_id, claimed_error,
            )
            if not claimed:
                return False
            kd = KingdeeClient(
                username=os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username,
                password=os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password,
            )
            await kd.login()
            doc = await _document(kd, bill_no)
            if not doc or doc[1] != rec["outstock_no"]:
                raise ValueError("出库单缺失、重复或关联不一致，停止本次补审")
            if doc[2] == "B":
                source = await kd.query(
                    "SAL_DELIVERYNOTICE", "FID,FDocumentStatus",
                    filter_string=f"FBillNo = '{bill_no}'", limit=2,
                )
                if not source or any(str(row[1]) != "C" for row in source):
                    raise ValueError("发货通知单未审核，停止本次补审")
                await kd.audit("SAL_OUTSTOCK", {"Ids": doc[0]})
                doc = await _document(kd, bill_no)
            if not doc or doc[1] != rec["outstock_no"] or doc[2] != "C":
                raise ValueError("复查销售出库单尚未审核，保留异常")
            await conn.execute(
                """UPDATE ads_sf_push_record SET status='auto_resolved',
                   error_message=$2, updated_at=now()
                   WHERE id=$1 AND status='outstock_failed' AND cancelled_at IS NULL
                     AND unaudit_detected_at IS NULL""",
                record_id, claimed_error + "\n补审结果: 正常Audit后复查金蝶状态C（或已审核）；未修改数量及GSP校验",
            )
            logger.info("SF补审恢复 bill=%s xsck=%s attempt=%d", bill_no, doc[1], attempt)
            return True
        except Exception as exc:
            logger.warning("SF补审保留异常 bill=%s: %s", bill_no, exc)
            if claimed_error is None:
                raise
            await conn.execute(
                """UPDATE ads_sf_push_record SET error_message=$2
                   WHERE id=$1 AND status='outstock_failed'""",
                record_id, claimed_error + f"\n补审结果: {str(exc)[:350]}"
                + ("；自动重试已达上限，需人工核查" if attempt >= MAX_ATTEMPTS else ""),
            )
            return False
        finally:
            try:
                if kd is not None:
                    await kd.aclose()
            finally:
                await conn.execute("SELECT pg_advisory_unlock($1)", key)


async def retry_failed_outstock_audits(*, budget_seconds: float = 30) -> dict:
    """At most three sequential documents per minute; never cancel an in-flight Audit."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch(
            """SELECT * FROM ads_sf_push_record
               WHERE bill_type='outbound' AND status='outstock_failed'
                 AND outstock_no IS NOT NULL AND cancelled_at IS NULL
                 AND unaudit_detected_at IS NULL AND error_message LIKE '%GSP%'
                 AND (error_message LIKE '%实发数量不能大于合格数量加不合格可出库数量%'
                      OR error_message LIKE '%存在还未复核完毕的%')
                 AND COALESCE(substring(error_message FROM '\\[sf-audit-retry:([0-9]+)\\]')::int,0) < 8
               ORDER BY updated_at ASC, id ASC LIMIT 100""",
        )
    started = time.monotonic()
    attempted = resolved = 0
    for rec in records:
        if attempted >= 3 or time.monotonic() - started >= budget_seconds:
            break
        if not retry_due(rec, datetime.now(timezone.utc)):
            continue
        try:
            resolved += bool(await _retry_one(pool, rec["id"]))
        except Exception:
            logger.exception("SF补审任务异常 record=%s", rec["id"])
        attempted += 1
    result = {"attempted": attempted, "resolved": resolved}
    logger.info("SF延时补审: %s", result)
    return result
