"""Bounded, single-attempt cancellation executor. Only the SF service talks to SF."""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from xml.etree import ElementTree as ET

from app.clients.kingdee import KingdeeClient
from app.config import settings
from app.db.pool import get_pool
from app.services.sf_interception_guard import heartbeat, lock_key, version
from app.services import sf_outbound

logger = logging.getLogger(__name__)
PRE_PACK = {"1100", "1600", "1700", "2000", "2300"}
REASONS = {
    "ONLY_EXACT_FHTZ_SUPPORTED": "仅支持明确的金蝶发货通知单",
    "HOLD_OR_RECORD_MISSING": "阻断或仓单记录不匹配",
    "ORDER_CHANGED_REVIEW_REQUIRED": "仓单状态已变化，请人工核实原请求",
    "ORDER_STATE_REQUIRES_MANUAL_REVIEW": "已有出库单或仓单状态不适合自动撤回",
    "SHIPMENT_CALLBACK_EXISTS": "已收到出库回调，需走人工退货核实",
    "PRIOR_CANCEL_ATTEMPT_REQUIRES_RECONCILIATION": "存在其他入口的取消尝试，必须先核实原结果",
    "XSCK_EXISTS": "金蝶已有销售出库单，不能自动撤仓",
    "XSCK_APPEARED_BEFORE_DISPATCH": "执行前发现新出库单，已停止取消",
    "WMS_ALREADY_CANCELLED": "仓库实时确认原单已取消，未再次发送取消",
    "WMS_NOT_BEFORE_PACKING": "仓库已打包、出库或状态不明，转人工处理",
    "WMS_CANCEL_CONFIRMED": "仓库实时确认已取消；库存是否释放仍需凭据",
    "CANCEL_UNCONFIRMED_OR_SHIPMENT_CONFLICT": "取消尚未确认或存在出库冲突，禁止换ID重发",
    "REMOTE_RESULT_UNKNOWN": "请求发出后的结果无法核实，需人工查询原请求",
    "EVIDENCE_UNAVAILABLE": "金蝶或顺丰证据查询失败，没有执行取消",
}


def wms_state(response: dict, bill: str) -> str:
    """Require an exact, unambiguous order in a successful response."""
    if response.get("head") != "OK":
        raise ValueError("WMS_QUERY_FAILED")
    root = ET.fromstring(response.get("raw", ""))
    orders = root.findall(".//SaleOrder")
    if len(orders) != 1:
        raise ValueError("WMS_ORDER_AMBIGUOUS")
    headers = orders[0].findall("Header")
    node = headers[0] if len(headers) == 1 else orders[0]
    if node.findtext("ErpOrder") != bill:
        raise ValueError("WMS_ORDER_MISMATCH")
    state = node.findtext("OrderStatus", "")
    if not state:
        raise ValueError("WMS_STATE_MISSING")
    return state


async def strict_xscks(kd, bill: str) -> list[dict]:
    # KingdeeClient.query historically turns a non-list error into []. Do not
    # use that convenience method to prove absence of an outstock document.
    query = json.dumps({
        "FormId": "SAL_OUTSTOCK", "FieldKeys": "FID,FBillNo,FDocumentStatus",
        "FilterString": f"FSrcBillNo = '{bill}'", "OrderString": "FID",
        "TopRowCount": 0, "StartRow": 0, "Limit": 501,
    })
    rows = await kd._call(
        "Kingdee.BOS.WebApi.ServicesStub.DynamicFormService.ExecuteBillQuery",
        {"data": query},
    )
    if not isinstance(rows, list) or len(rows) >= 501 or any(
        not isinstance(r, list) or len(r) != 3 or not r[0] or not r[1]
        for r in rows
    ):
        raise ValueError("XSCK_EVIDENCE_INCOMPLETE")
    docs = {str(r[1]): {"id": str(r[0]), "bill_no": str(r[1]),
                       "status": str(r[2])} for r in rows}
    return list(docs.values())


async def finish(pool, op, status, reason, *, sent="NO", evidence=None, receipt=None):
    result = {"reason": reason, "reason_message": REASONS.get(reason, reason), "hold_active": True,
              "inventory_released": "UNKNOWN", **(evidence or {})}
    if receipt:
        result["receipt_sha256"] = hashlib.sha256(receipt.encode()).hexdigest()
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE sf_api_operation SET status=$2,request_sent=$3,result=$4::jsonb,
               receipt=coalesce($5,receipt),updated_at=now() WHERE operation_id=$1""",
            op["operation_id"], status, sent, json.dumps(result), receipt,
        )


async def shipment_evidence(conn, bill):
    return await conn.fetchval(
        """SELECT EXISTS(SELECT 1 FROM ods_sf_push_log WHERE erp_order=$1
           AND service_code='SALE_ORDER_OUTBOUND_DETAIL_PUSH_SERVICE')""", bill,
    )


async def execute_one(pool, op):
    bill = op["bill_no"]
    # In-process calls and persisted commands are both validated.
    if not re.fullmatch(r"FHTZ[A-Za-z0-9._-]{1,95}", bill):
        await finish(pool, op, "BLOCKED", "ONLY_EXACT_FHTZ_SUPPORTED")
        return
    async with pool.acquire() as conn, conn.transaction():
        if not await conn.fetchval(
            "SELECT pg_try_advisory_xact_lock($1)", lock_key(bill),
        ):
            return
        # The claim must commit BEFORE any external action, independently of
        # the transaction holding the order lock. A crash is never replayed.
        async with pool.acquire() as journal:
            claimed = await journal.fetchval(
                """UPDATE sf_api_operation SET status='PROCESSING',updated_at=now()
                   WHERE operation_id=$1 AND status='QUEUED' RETURNING operation_id""",
                op["operation_id"],
            )
        if not claimed:
            return
        kd = None
        sent = False
        received = False
        try:
            hold = await conn.fetchrow("SELECT * FROM sf_api_hold WHERE bill_no=$1", bill)
            rec = await conn.fetchrow(
                "SELECT * FROM ads_sf_push_record WHERE bill_no=$1 AND bill_type='outbound'",
                bill,
            )
            if not hold or hold["owner"] != op["owner"] or not rec:
                await finish(pool, op, "BLOCKED", "HOLD_OR_RECORD_MISSING")
                return
            if op["expected_version"] != version(rec):
                await finish(pool, op, "BLOCKED", "ORDER_CHANGED_REVIEW_REQUIRED")
                return
            if (rec["erp_order"] != bill or rec["outstock_no"] or rec["cancelled_at"]
                or rec.get("unaudit_detected_at") or rec["status"] not in (
                "success", "timeout_alert",
            )):
                await finish(pool, op, "BLOCKED", "ORDER_STATE_REQUIRES_MANUAL_REVIEW")
                return
            if await shipment_evidence(conn, bill):
                await finish(pool, op, "BLOCKED", "SHIPMENT_CALLBACK_EXISTS")
                return
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM sf_api_legacy_cancel WHERE bill_no=$1)", bill,
            ):
                await finish(pool, op, "BLOCKED", "PRIOR_CANCEL_ATTEMPT_REQUIRES_RECONCILIATION")
                return
            kd = KingdeeClient(
                username=os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username,
                password=os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password,
            )
            await kd.login()
            xscks = await strict_xscks(kd, bill)
            if xscks:
                await finish(pool, op, "BLOCKED", "XSCK_EXISTS", evidence={"xscks": xscks})
                return
            state = wms_state(await sf_outbound.query_outbound_status(bill), bill)
            if state == "1400":
                await finish(pool, op, "SUCCEEDED", "WMS_ALREADY_CANCELLED",
                             evidence={"wms_state": state, "xscks": []})
                return
            if state not in PRE_PACK:
                await finish(pool, op, "BLOCKED", "WMS_NOT_BEFORE_PACKING",
                             evidence={"wms_state": state})
                return
            # Recheck after reads, immediately before dispatch; KD actions by
            # other systems cannot share our lock, so return any later conflict.
            if await shipment_evidence(conn, bill):
                await finish(pool, op, "BLOCKED", "SHIPMENT_CALLBACK_EXISTS")
                return
            if await strict_xscks(kd, bill):
                await finish(pool, op, "BLOCKED", "XSCK_APPEARED_BEFORE_DISPATCH")
                return
            async with pool.acquire() as journal:
                await journal.execute(
                    """UPDATE sf_api_operation SET request_sent='UNKNOWN',updated_at=now()
                       WHERE operation_id=$1""", op["operation_id"],
                )
            sent = True
            content = sf_outbound._build_xml(
                "CANCEL_SALE_ORDER_SERVICE",
                "<CancelSaleOrderRequest>"
                f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
                "<Remark>ERP退款阻断受控撤回</Remark><SaleOrders><SaleOrder>"
                f"<ErpOrder>{bill}</ErpOrder></SaleOrder></SaleOrders></CancelSaleOrderRequest>",
            )
            response = await sf_outbound._send_request_unchecked("CANCEL_SALE_ORDER_SERVICE", content)
            receipt = response.get("raw", "")
            received = True
            async with pool.acquire() as journal:
                await journal.execute(
                    """UPDATE sf_api_operation SET request_sent='YES',receipt=$2,
                       updated_at=now() WHERE operation_id=$1""", op["operation_id"], receipt,
                )
            # A transport success/Head OK is not proof of cancellation. Verify
            # exact warehouse state and retain the original response privately.
            after = wms_state(await sf_outbound.query_outbound_status(bill), bill)
            xscks_after = await strict_xscks(kd, bill)
            conflict = xscks_after or await shipment_evidence(conn, bill)
            if after == "1400" and not conflict:
                await finish(pool, op, "SUCCEEDED", "WMS_CANCEL_CONFIRMED", sent="YES",
                             evidence={"wms_state": after, "xscks": []}, receipt=receipt)
            else:
                await finish(pool, op, "UNKNOWN", "CANCEL_UNCONFIRMED_OR_SHIPMENT_CONFLICT",
                             sent="YES", evidence={"wms_state": after, "xscks": xscks_after},
                             receipt=receipt)
            # Do not overwrite sync status, outstock, inventory or callbacks.
        except Exception:
            logger.exception("SF interception operation %s failed (sent=%s)", op["operation_id"], sent)
            await finish(pool, op, "UNKNOWN" if sent else "BLOCKED",
                         "REMOTE_RESULT_UNKNOWN" if sent else "EVIDENCE_UNAVAILABLE",
                         sent="YES" if received else ("UNKNOWN" if sent else "NO"))
        finally:
            if kd:
                await kd.aclose()


async def run():
    await heartbeat()
    pool = await get_pool()
    async with pool.acquire() as conn:
        # No automatic replay even if shutdown interrupted a read before send.
        await conn.execute(
            """UPDATE sf_api_operation SET status='UNKNOWN',updated_at=now(),
               result='{"reason":"WORKER_INTERRUPTED_RECONCILE_MANUALLY","hold_active":true}'::jsonb
               WHERE status='PROCESSING' AND updated_at < now()-interval '10 minutes'"""
        )
        ops = await conn.fetch(
            """SELECT * FROM sf_api_operation WHERE kind='CANCEL' AND status='QUEUED'
               ORDER BY created_at LIMIT 2"""
        )
    start = time.monotonic()
    for op in ops:
        if time.monotonic() - start > 40:
            break
        await execute_one(pool, op)
        await asyncio.sleep(0.3)
    await heartbeat()
