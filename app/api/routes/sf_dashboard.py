"""顺丰中控平台 API

金蝶↔顺丰 WMS 的推送状态监控、取消操作、异常排查、WMS 作业状态查询。
"""

from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.db.pool import get_pool, row_to_dict
from app.services import sf_item_push

router = APIRouter(prefix="/api/sf/dashboard", tags=["顺丰中控平台"])
logger = logging.getLogger("union")
TZ_CN = timezone(timedelta(hours=8))

WMS_STATUS_MAP = {
    "1100": "已接单",
    "1400": "已取消",
    "1600": "已提交仓库",
    "1700": "已下发WMS",
    "2000": "仓库准备中",
    "2300": "拣货中",
    "2400": "已检验打包",
    "2900": "已出库",
    "3900": "已发货",
    "3002": "快递已揽收",
    "3003": "快递已揽收",
    "2001": "中转分拣",
    "2002": "到达网点",
    "2006": "派件中",
    "2003": "已签收",
    "2009": "运输中",
    "10006": "客户取件",
    "11139": "中转中",
}

STATUS_LABELS = {
    "pending": "待推送",
    "success": "已推送",
    "failed": "推送失败",
    "callback_ok": "回调通过",
    "callback_mismatch": "数量异常",
    "timeout_alert": "超时未回调",
    "outstock_created": "出库单已建",
    "outstock_created_no_callback": "出库单已建·回调缺失",
    "outstock_failed": "出库单失败",
    "cancelled": "已取消",
    "cancelled_by_unaudit": "反审核·已自动撤回",
    "unaudit_cancel_failed": "反审核·撤回失败",
    "auto_resolved": "已自动通过",
    "manual_resolved": "已人工确认",
}

# 异常状态集合：阿潘说"金蝶处理过就不应该再显示异常"覆盖的范围
# outstock_created_no_callback 也算异常（业务已补救但通道有问题，需要对账+找顺丰排查）
# unaudit_cancel_failed 是新增的高优异常：金蝶反审核但顺丰撤不动（很可能已发货/已收货）
ABNORMAL_STATUSES = (
    "outstock_failed",
    "callback_mismatch",
    "timeout_alert",
    "outstock_created_no_callback",
    "unaudit_cancel_failed",
)

BILL_TYPE_LABELS = {
    "transfer": "直接调拨单",
    "transfer_in": "分布式调入单",
    "purchase_in": "采购入库单",
    "outbound": "GSP发货通知单",
}

INBOUND_BILL_TYPES = {"transfer", "transfer_in", "purchase_in"}

CARRIER_NAME_MAP = {
    "CP": "顺丰速运",
    "SF": "顺丰速运",
    "JD": "京东物流",
    "JDKD": "京东物流",
    "YTO": "圆通快递",
    "YT": "圆通快递",
    "ZTO": "中通快递",
    "ZT": "客户自提",
}

WAYBILL_CARRIER_PREFIXES: tuple[tuple[str, str], ...] = (
    ("JDVC", "JDKD"),
    ("JDL", "JDKD"),
    ("JD", "JDKD"),
    ("SF", "CP"),
    ("YT", "YTO"),
)


def _carrier_name(carrier_code: str) -> str:
    code = (carrier_code or "").strip().upper()
    return CARRIER_NAME_MAP.get(code, "")


def _infer_carrier_from_waybill(waybill_no: str) -> str:
    normalized = (waybill_no or "").strip().upper()
    for prefix, carrier_code in WAYBILL_CARRIER_PREFIXES:
        if normalized.startswith(prefix):
            return carrier_code
    return ""


def _resolve_carrier(waybill_no: str, carrier_code: str) -> str:
    code = (carrier_code or "").strip().upper()
    if code == "JD":
        code = "JDKD"
    return _infer_carrier_from_waybill(waybill_no) or code


@router.get("/stats")
async def get_stats():
    """统计概览：各状态计数 + 最近推送时间"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT status, count(*) AS cnt FROM ads_sf_push_record GROUP BY status"
        )
        total = await conn.fetchval("SELECT count(*) FROM ads_sf_push_record")
        latest = await conn.fetchval(
            "SELECT max(created_at) FROM ads_sf_push_record"
        )
        type_rows = await conn.fetch(
            "SELECT bill_type, count(*) AS cnt FROM ads_sf_push_record GROUP BY bill_type"
        )
        # WMS 状态分布（只统计出库 + 近 14 天，避免历史干扰）
        wms_rows = await conn.fetch(
            """
            SELECT COALESCE(NULLIF(sf_wms_status_text, ''), '__empty__') AS wms_text,
                   count(*) AS cnt
            FROM ads_sf_push_record
            WHERE bill_type = 'outbound'
              AND created_at > NOW() - INTERVAL '14 days'
              AND status IN ('success','callback_ok','outstock_created',
                             'callback_mismatch','timeout_alert','outstock_failed')
            GROUP BY wms_text
            ORDER BY cnt DESC
            """
        )

    status_counts = {r["status"]: r["cnt"] for r in rows}
    type_counts = {r["bill_type"]: r["cnt"] for r in type_rows}
    wms_status_counts = {r["wms_text"]: r["cnt"] for r in wms_rows}

    needs_attention = (
        status_counts.get("failed", 0)
        + status_counts.get("callback_mismatch", 0)
        + status_counts.get("timeout_alert", 0)
        + status_counts.get("outstock_failed", 0)
        + status_counts.get("unaudit_cancel_failed", 0)
    )

    return {
        "total": total or 0,
        "needs_attention": needs_attention,
        "status_counts": status_counts,
        "type_counts": type_counts,
        "status_labels": STATUS_LABELS,
        "bill_type_labels": BILL_TYPE_LABELS,
        "wms_status_counts": wms_status_counts,
        "wms_status_empty_label": "未同步",
        "latest_push_at": latest.isoformat() if latest else None,
    }


STATUS_GROUPS = {
    "needs_attention": [
        "failed",
        "callback_mismatch",
        "timeout_alert",
        "outstock_failed",
        "outstock_created_no_callback",
        "unaudit_cancel_failed",
    ],
    "all_failed": ["failed", "outstock_failed", "unaudit_cancel_failed"],
    "all_ok": ["callback_ok", "outstock_created", "auto_resolved", "manual_resolved", "cancelled_by_unaudit"],
    "inbound": [],
    "unaudit": ["cancelled_by_unaudit", "unaudit_cancel_failed"],
}


@router.get("/records")
async def list_records(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: str | None = None,
    status_group: str | None = None,
    bill_type: str | None = None,
    wms_status: str | None = None,
    search: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
):
    """分页查询推送记录，支持状态/类型/WMS状态/日期/关键词筛选"""
    pool = await get_pool()
    conditions: list[str] = []
    params: list = []
    idx = 1

    if status_group and status_group in STATUS_GROUPS:
        group_statuses = STATUS_GROUPS[status_group]
        if group_statuses:
            placeholders = ", ".join(f"${idx + i}" for i in range(len(group_statuses)))
            conditions.append(f"r.status IN ({placeholders})")
            params.extend(group_statuses)
            idx += len(group_statuses)
        if status_group == "inbound":
            conditions.append(f"r.bill_type IN (${idx}, ${idx+1}, ${idx+2})")
            params.extend(["transfer", "transfer_in", "purchase_in"])
            idx += 3
    elif status:
        conditions.append(f"r.status = ${idx}")
        params.append(status)
        idx += 1
    if bill_type:
        conditions.append(f"r.bill_type = ${idx}")
        params.append(bill_type)
        idx += 1
    if wms_status:
        # 特殊值 __empty__ = 未同步 WMS 状态（NULL 或空字符串）
        if wms_status == "__empty__":
            conditions.append("(r.sf_wms_status_text IS NULL OR r.sf_wms_status_text = '')")
        else:
            conditions.append(f"r.sf_wms_status_text = ${idx}")
            params.append(wms_status)
            idx += 1
    if search:
        # 支持批量查询：用逗号/分号/换行/空格分隔多个单号
        # 单个值 → ILIKE 模糊；多个值 → 精确 IN（仓库粘贴一列单号即可批量查）
        import re
        tokens = [t for t in re.split(r"[,;\s\n\r\t]+", search.strip()) if t]
        if len(tokens) >= 2:
            placeholders = ", ".join(f"${idx + i}" for i in range(len(tokens)))
            # 同一组占位符复用在三个 IN 子句
            conditions.append(
                f"(r.bill_no IN ({placeholders}) "
                f"OR r.erp_order IN ({placeholders}) "
                f"OR r.outstock_no IN ({placeholders}))"
            )
            params.extend(tokens)
            idx += len(tokens)
            # 批量查询时自动放大 page_size 避免被截断
            page_size = max(page_size, min(len(tokens), 100))
        else:
            conditions.append(f"(r.bill_no ILIKE ${idx} OR r.erp_order ILIKE ${idx} OR r.outstock_no ILIKE ${idx})")
            params.append(f"%{search}%")
            idx += 1
    if date_from:
        conditions.append(f"r.created_at >= ${idx}::timestamptz")
        params.append(date_from)
        idx += 1
    if date_to:
        conditions.append(f"r.created_at < (${idx}::date + 1)")
        params.append(date_to)
        idx += 1

    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""

    async with pool.acquire() as conn:
        total = await conn.fetchval(
            f"SELECT count(*) FROM ads_sf_push_record r{where}", *params
        )
        rows = await conn.fetch(
            f"""SELECT r.id, r.bill_no, r.bill_type, r.erp_order,
                       r.sf_receipt_id, r.status, r.error_message,
                       r.outstock_no, r.fhtz_fid,
                       r.carrier_code, r.carrier_name, r.waybill_no,
                       r.sf_wms_status, r.sf_wms_status_text,
                       r.created_at, r.updated_at,
                       (
                         SELECT COUNT(*)::int FROM ods_sf_push_log L
                         WHERE L.erp_order = r.bill_no
                            OR (NULLIF(TRIM(r.erp_order), '') IS NOT NULL
                                AND L.erp_order = r.erp_order)
                            OR (NULLIF(TRIM(r.outstock_no), '') IS NOT NULL
                                AND L.erp_order = r.outstock_no)
                       ) AS callback_count
                FROM ads_sf_push_record r{where}
                ORDER BY r.created_at DESC
                LIMIT ${idx} OFFSET ${idx + 1}""",
            *params, page_size, (page - 1) * page_size,
        )

    items = []
    for r in rows:
        d = row_to_dict(r)
        d["status_label"] = STATUS_LABELS.get(d.get("status", ""), d.get("status", ""))
        d["bill_type_label"] = BILL_TYPE_LABELS.get(d.get("bill_type", ""), d.get("bill_type", ""))
        items.append(d)

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.get("/records/{record_id}")
async def get_record_detail(record_id: int):
    """单条记录详情，含推送商品清单 + 回调原始报文"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT * FROM ads_sf_push_record WHERE id = $1", record_id
        )
        if not rec:
            raise HTTPException(404, "记录不存在")

        callbacks = await conn.fetch(
            """SELECT id, service_code, erp_order, raw_payload, status, received_at
               FROM ods_sf_push_log
               WHERE erp_order = $1
                  OR ($2::text IS NOT NULL AND TRIM($2::text) <> ''
                      AND erp_order = $2::text)
                  OR ($3::text IS NOT NULL AND TRIM($3::text) <> ''
                      AND erp_order = $3::text)
               ORDER BY received_at DESC
               LIMIT 50""",
            rec["bill_no"],
            rec["erp_order"],
            rec["outstock_no"],
        )

    d = row_to_dict(rec)
    d["status_label"] = STATUS_LABELS.get(d.get("status", ""), d.get("status", ""))
    d["bill_type_label"] = BILL_TYPE_LABELS.get(d.get("bill_type", ""), d.get("bill_type", ""))

    items = []
    if d.get("items_json"):
        try:
            items = json.loads(d["items_json"]) if isinstance(d["items_json"], str) else d["items_json"]
        except (json.JSONDecodeError, TypeError):
            pass
    d["items"] = items

    d["callbacks"] = [row_to_dict(c) for c in callbacks]

    return d


class CancelRequest(BaseModel):
    reason: str = ""


@router.post("/records/{record_id}/cancel")
async def cancel_record(record_id: int, body: CancelRequest | None = None):
    """取消推送单据（调用顺丰 CANCEL 接口，区分出库/入库）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT id, bill_no, erp_order, status, bill_type, sf_receipt_id FROM ads_sf_push_record WHERE id = $1",
            record_id,
        )
    if not rec:
        raise HTTPException(404, "记录不存在")

    if rec["status"] in ("outstock_created",):
        raise HTTPException(400, "已创建出库单的记录不可取消，请在金蝶中操作")

    bill_type = rec["bill_type"]
    erp_order = rec["erp_order"]

    try:
        if bill_type == "outbound":
            from app.services.sf_outbound import cancel_outbound_order
            result = await cancel_outbound_order(erp_order, rec.get("sf_receipt_id") or "")
        elif bill_type in INBOUND_BILL_TYPES:
            from app.services.sf_automation import _sf_xml, _sf_send
            cancel_body = (
                "<CancelPurchaseOrderRequest>"
                f"<CompanyCode>{_get_sf_company_code()}</CompanyCode>"
                "<PurchaseOrders><PurchaseOrder>"
                f"<ErpOrder>{erp_order}</ErpOrder>"
                f"<WarehouseCode>{_get_sf_warehouse_code()}</WarehouseCode>"
                "</PurchaseOrder></PurchaseOrders>"
                "</CancelPurchaseOrderRequest>"
            )
            content = _sf_xml("CANCEL_PURCHASE_ORDER_SERVICE", cancel_body)
            ok, _, raw = await _sf_send("CANCEL_PURCHASE_ORDER_SERVICE", content)
            result = {"success": ok, "raw": raw[:500]}
        else:
            raise HTTPException(400, f"未知单据类型: {bill_type}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"顺丰取消接口调用失败: {e}")

    cancel_ok = False
    raw_result = ""
    if isinstance(result, dict):
        cancel_ok = bool(result.get("success")) or result.get("head") == "OK"
        raw_result = str(result.get("raw") or result.get("error") or result)
    if not cancel_ok:
        logger.warning(
            "顺丰取消失败，拒绝标记 cancelled: record_id=%s erp_order=%s result=%s",
            record_id,
            erp_order,
            raw_result[:500],
        )
        raise HTTPException(502, f"顺丰取消失败，未修改本地状态: {raw_result[:300]}")

    reason = body.reason if body and body.reason else "中控平台手动取消"
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE ads_sf_push_record
               SET status = 'cancelled',
                   error_message = $1,
                   sf_response = COALESCE(NULLIF($2, ''), sf_response),
                   cancelled_at = now(),
                   updated_at = now()
               WHERE id = $3""",
            reason,
            raw_result[:2000],
            record_id,
        )

    return {"success": True, "message": f"已取消 {erp_order}", "sf_result": result}


@router.post("/records/{record_id}/retry")
async def retry_record(record_id: int):
    """立即重推失败的记录（从金蝶重查数据，直接推送到顺丰）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT id, status FROM ads_sf_push_record WHERE id = $1", record_id
        )
        if not rec:
            raise HTTPException(404, "记录不存在")
        if rec["status"] not in ("failed", "outstock_failed", "timeout_alert", "callback_mismatch", "cancelled", "cancelled_by_unaudit", "outstock_created_no_callback"):
            raise HTTPException(400, f"当前状态 {rec['status']} 不支持重试")

    from app.services.sf_automation import retry_single_push

    try:
        result = await retry_single_push(record_id)
    except Exception as e:
        logger.exception("重推 record_id=%d 异常", record_id)
        raise HTTPException(502, f"重推失败: {e}")

    return result


@router.get("/records/{record_id}/wms-status")
async def get_wms_status(record_id: int):
    """实时查询顺丰 WMS 作业状态（调用 SALE_ORDER_STATUS_QUERY_SERVICE）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT id, bill_no, bill_type FROM ads_sf_push_record WHERE id = $1",
            record_id,
        )
    if not rec:
        raise HTTPException(404, "记录不存在")
    if rec["bill_type"] != "outbound":
        raise HTTPException(400, "仅出库单支持 WMS 状态查询")

    from app.services.sf_outbound import query_outbound_status
    try:
        result = await query_outbound_status(rec["bill_no"])
    except Exception as e:
        raise HTTPException(502, f"顺丰状态查询失败: {e}")

    if result.get("head") != "OK":
        return {"success": False, "message": "顺丰返回异常", "raw": result.get("raw", "")[:500]}

    raw_xml = result.get("raw", "")
    steps = _parse_wms_steps(raw_xml)
    header = _parse_wms_header(raw_xml)

    current_status = header.get("OrderStatus", "")
    current_text = WMS_STATUS_MAP.get(current_status, f"未知({current_status})")
    waybill = header.get("WayBillNo", "")
    carrier = _resolve_carrier(waybill, header.get("Carrier", ""))
    carrier_name = _carrier_name(carrier)

    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE ads_sf_push_record
               SET sf_wms_status = $1, sf_wms_status_text = $2,
                   waybill_no = COALESCE(NULLIF($3, ''), waybill_no),
                   carrier_code = COALESCE(NULLIF($4, ''), carrier_code),
                   carrier_name = COALESCE(NULLIF($5, ''), carrier_name),
                   updated_at = now()
               WHERE id = $6""",
            current_status, current_text, waybill, carrier, carrier_name, record_id,
        )

    return {
        "success": True,
        "order_status": current_status,
        "order_status_text": current_text,
        "waybill_no": waybill,
        "carrier": carrier,
        "carrier_product": header.get("CarrierProduct", ""),
        "create_time": header.get("CreateTime", ""),
        "wms_push_time": header.get("WmsPushTime", ""),
        "wms_complete_time": header.get("WmsCompleteTime", ""),
        "shipment_id": header.get("ShipmentId", ""),
        "steps": steps,
    }


def _parse_wms_header(raw_xml: str) -> dict[str, str]:
    """从 STATUS_QUERY 响应中提取 Header 字段"""
    result: dict[str, str] = {}
    try:
        root = ET.fromstring(raw_xml)
        header = root.find(".//Header")
        if header is not None:
            for el in header:
                if el.text and el.text.strip():
                    result[el.tag] = el.text.strip()
    except ET.ParseError:
        pass
    return result


def _parse_wms_steps(raw_xml: str) -> list[dict[str, str]]:
    """从 STATUS_QUERY 响应中提取 Steps 步骤链"""
    steps: list[dict[str, str]] = []
    try:
        root = ET.fromstring(raw_xml)
        for step in root.iter("Step"):
            event_time = step.findtext("EventTime", "")
            status_code = step.findtext("Status", "")
            note = step.findtext("Note", "")
            address = step.findtext("EventAddress", "")
            status_text = WMS_STATUS_MAP.get(status_code, status_code)
            steps.append({
                "time": event_time,
                "status": status_code,
                "status_text": status_text,
                "note": note,
                "address": address,
            })
    except ET.ParseError:
        pass
    seen = set()
    deduped = []
    for s in steps:
        key = f"{s['time']}_{s['status']}"
        if key not in seen:
            seen.add(key)
            deduped.append(s)
    return deduped


def _get_sf_company_code() -> str:
    from app.config import settings
    return settings.sf_company_code

def _get_sf_warehouse_code() -> str:
    from app.config import settings
    return settings.sf_warehouse_code


# ──────────────────────────────────────────────
# WMS 状态批量同步（2026-04-22 新增，2026-04-29 修复）
# 解决 sf-dashboard 列表「推送信息」列大面积为空的问题
# 根因v1：sf_wms_status_text 字段原先只在手动点「查询 WMS」按钮时才写入
# 根因v2（2026-04-29）：only_empty=True 导致一旦填入中间状态（如"仓库准备中"）
#   就不再刷新，WMS 从中间态变成"已出库"后我方永远不知道 → WMS兜底永远无法触发
# 修复：除了刷空值，也刷 status='success' 且 WMS 处于非终态的记录（已出库/已取消 = 终态）
# ──────────────────────────────────────────────

# WMS 终态：已出库、已取消 — 这些不需要再刷新
_WMS_TERMINAL_TEXTS = {'已出库', '已发货', '已取消'}

async def sync_wms_status_batch(limit: int = 30, only_empty: bool = True) -> dict:
    """批量调顺丰 STATUS_QUERY,把 WMS 状态回写到 ads_sf_push_record

    扫描优先级：
    1. sf_wms_status_text 为空的记录 → 首次填入
    2. status='success' 且 WMS 不是终态（已出库/已取消）且超过 10 分钟没更新 → 推进进度
       这些记录需要定期刷新，直到 WMS 变成"已出库"后由 validate_outbound_callbacks 的
       WMS兜底逻辑自动创建金蝶出库单
    3. only_empty=False 时：所有非终态 + 超过 2 小时没更新的也刷

    内置限流(每次调用间隔 1.1s),避免触发顺丰 60次/分 限频
    """
    import asyncio
    from app.services.sf_outbound import query_outbound_status

    pool = await get_pool()

    # 构建 WHERE 子句：空值 + 非终态刷新
    if only_empty:
        # 优先刷空值，然后刷 success 状态中 WMS 非终态（需要推进的记录）
        where_stale = (
            "AND ("
            "  (sf_wms_status_text IS NULL OR sf_wms_status_text = '')"
            "  OR ("
            "    status IN ('success', 'timeout_alert')"
            "    AND sf_wms_status_text NOT IN ('已出库', '已发货', '已取消')"
            "    AND updated_at < NOW() - INTERVAL '10 minutes'"
            "  )"
            ")"
        )
    else:
        where_stale = (
            "AND ("
            "  (sf_wms_status_text IS NULL OR sf_wms_status_text = '')"
            "  OR ("
            "    sf_wms_status_text NOT IN ('已出库', '已发货', '已取消')"
            "    AND updated_at < NOW() - INTERVAL '10 minutes'"
            "  )"
            "  OR updated_at < NOW() - INTERVAL '2 hours'"
            ")"
        )

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, bill_no, status, sf_wms_status_text FROM ads_sf_push_record
            WHERE bill_type = 'outbound'
              AND status IN ('success', 'callback_ok', 'outstock_created',
                             'callback_mismatch', 'timeout_alert')
              AND created_at > NOW() - INTERVAL '14 days'
              {where_stale}
            ORDER BY
              (sf_wms_status_text IS NULL OR sf_wms_status_text = '') DESC,
              created_at DESC
            LIMIT $1
            """,
            limit,
        )

    if not rows:
        return {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    success = 0
    failed = 0
    skipped = 0

    for r in rows:
        try:
            result = await query_outbound_status(r["bill_no"])
            if result.get("head") != "OK":
                failed += 1
                await asyncio.sleep(1.1)
                continue

            raw_xml = result.get("raw", "")
            header = _parse_wms_header(raw_xml)
            current_status = header.get("OrderStatus", "")
            if not current_status:
                skipped += 1
                await asyncio.sleep(1.1)
                continue

            current_text = WMS_STATUS_MAP.get(current_status, f"未知({current_status})")
            waybill = header.get("WayBillNo", "")
            carrier = _resolve_carrier(waybill, header.get("Carrier", ""))
            carrier_name = _carrier_name(carrier)

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE ads_sf_push_record
                    SET sf_wms_status = $1,
                        sf_wms_status_text = $2,
                        waybill_no = COALESCE(NULLIF($3, ''), waybill_no),
                        carrier_code = COALESCE(NULLIF($4, ''), carrier_code),
                        carrier_name = COALESCE(NULLIF($5, ''), carrier_name),
                        updated_at = now()
                    WHERE id = $6
                    """,
                    current_status, current_text, waybill, carrier, carrier_name, r["id"],
                )
            success += 1
        except Exception as e:
            logger.warning("WMS 状态同步 %s 失败: %s", r["bill_no"], e)
            failed += 1

        await asyncio.sleep(1.1)

    return {
        "total": len(rows),
        "success": success,
        "failed": failed,
        "skipped": skipped,
    }


# ──────────────────────────────────────────────
# 金蝶复检 + 手动标记已处理（2026-04-27 阿潘需求）
# 阿潘场景：顺丰回调挂掉 → 状态进入 outstock_failed/callback_mismatch
# 但阿潘已经在金蝶 UI 里手动救回 XSCK 出库单（匹配库存出库 → 反审 → 重新审核）
# 之前 dashboard 不会感知，一直挂"异常"看着不爽
# 解决方案：
#   ① 自动复检：对 abnormal 状态，去金蝶反查 FHTZ 是否已下推出已审核的 XSCK
#      命中 → 自动转 auto_resolved（区别于代码自己跑通的 outstock_created）
#   ② 手动确认：阿潘自己点按钮强制标 manual_resolved，留备注+操作员审计
# ──────────────────────────────────────────────


async def recheck_kingdee_status_for_abnormal(limit: int = 50) -> dict:
    """复检异常单：去金蝶查 FHTZ 是否已下推出已审核的 XSCK，命中就自动转 auto_resolved

    - 只扫 outbound + abnormal 状态 + 最近 14 天
    - 调金蝶 API 限流：每条间隔 0.5s
    - 多张 XSCK 不处理（已有 duplicate_outstock 状态走告警链路）
    - ⚠ outstock_created_no_callback（顺丰回调缺失）不参与自动复检：
      虽然金蝶里 XSCK 已审核，但通道异常需要阿潘人工对账后点「人工确认」销账，
      不能自动洗白，否则下次还会再发生回调缺失也察觉不到。
    """
    import asyncio
    import os
    from app.clients.kingdee import KingdeeClient
    from app.config import settings

    pool = await get_pool()

    # 自动复检参与的状态：排除 outstock_created_no_callback（要求人工确认）
    auto_recheck_statuses = [
        s for s in ABNORMAL_STATUSES if s != "outstock_created_no_callback"
    ]

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, bill_no FROM ads_sf_push_record
            WHERE bill_type = 'outbound'
              AND status = ANY($1::text[])
              AND created_at > NOW() - INTERVAL '14 days'
            ORDER BY created_at DESC
            LIMIT $2
            """,
            auto_recheck_statuses, limit,
        )

    if not rows:
        return {"total": 0, "resolved": 0, "still_abnormal": 0, "errors": 0}

    kd_user = os.getenv("KINGDEE_SF_USERNAME") or settings.kingdee_username or ""
    kd_pass = os.getenv("KINGDEE_SF_PASSWORD") or settings.kingdee_password or ""
    kingdee = KingdeeClient(username=kd_user, password=kd_pass)
    await kingdee.login()

    resolved = 0
    still_abnormal = 0
    errors = 0
    resolved_bills: list[str] = []

    for r in rows:
        bill_no = r["bill_no"]
        try:
            existing = await kingdee.query(
                "SAL_OUTSTOCK", "FID,FBillNo,FDocumentStatus",
                filter_string=f"FSrcBillNo = '{bill_no}'",
                limit=5,
            )
            unique_xscks: dict[str, str] = {}
            if existing:
                for row in existing:
                    if isinstance(row, list) and len(row) >= 3:
                        xb = str(row[1])
                        if xb and xb not in unique_xscks:
                            unique_xscks[xb] = str(row[2])

            if len(unique_xscks) == 1:
                xb, doc_status = next(iter(unique_xscks.items()))
                if doc_status == "C":
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """UPDATE ads_sf_push_record
                               SET status = 'auto_resolved',
                                   outstock_no = COALESCE(outstock_no, $1),
                                   error_message = COALESCE(error_message, '')
                                       || E'\\n[金蝶复检 ' || to_char(now() AT TIME ZONE 'Asia/Shanghai', 'MM-DD HH24:MI')
                                       || '] 金蝶已审核 XSCK ' || $1 || '，自动确认正常',
                                   updated_at = now()
                               WHERE id = $2""",
                            xb, r["id"],
                        )
                    resolved += 1
                    resolved_bills.append(bill_no)
                    logger.info("金蝶复检通过: FHTZ %s → XSCK %s 已审核，自动 auto_resolved", bill_no, xb)
                else:
                    still_abnormal += 1
            else:
                still_abnormal += 1
        except Exception as e:
            logger.warning("金蝶复检 %s 失败: %s", bill_no, e)
            errors += 1

        await asyncio.sleep(0.5)

    return {
        "total": len(rows),
        "resolved": resolved,
        "still_abnormal": still_abnormal,
        "errors": errors,
        "resolved_bills": resolved_bills[:20],
    }


@router.post("/recheck-kingdee")
async def trigger_recheck_kingdee(limit: int = Query(80, ge=1, le=300)):
    """手动触发金蝶复检 — 把已经在金蝶 UI 里救回的异常单自动转 auto_resolved"""
    try:
        return await recheck_kingdee_status_for_abnormal(limit=limit)
    except Exception as e:
        logger.exception("金蝶复检失败")
        raise HTTPException(500, f"金蝶复检失败: {e}")


class MarkResolvedRequest(BaseModel):
    note: str = ""
    operator: str = ""


@router.post("/records/{record_id}/mark-resolved")
async def mark_record_resolved(record_id: int, body: MarkResolvedRequest | None = None):
    """手动强制标记记录为「已人工确认」— 用于金蝶已处理但代码逻辑还判定异常的兜底"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT id, bill_no, status FROM ads_sf_push_record WHERE id = $1",
            record_id,
        )
    if not rec:
        raise HTTPException(404, "记录不存在")

    if rec["status"] not in ABNORMAL_STATUSES:
        raise HTTPException(
            400,
            f"当前状态 {rec['status']} 不需要人工确认（仅异常单 {'/'.join(ABNORMAL_STATUSES)} 可点）",
        )

    note = (body.note if body else "").strip() or "金蝶已手动处理"
    operator = (body.operator if body else "").strip() or "未知"
    stamp = datetime.now(TZ_CN).strftime("%m-%d %H:%M")
    audit_line = f"\n[手动确认 by {operator} {stamp}] {note}"

    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE ads_sf_push_record
               SET status = 'manual_resolved',
                   error_message = COALESCE(error_message, '') || $1,
                   updated_at = now()
               WHERE id = $2""",
            audit_line, record_id,
        )

    return {"success": True, "message": f"已确认 {rec['bill_no']}", "operator": operator, "note": note}


@router.post("/sync-wms-status")
async def trigger_sync_wms_status(
    limit: int = Query(50, ge=1, le=200),
    only_empty: bool = Query(True),
):
    """手动触发批量同步 WMS 状态"""
    try:
        return await sync_wms_status_batch(limit=limit, only_empty=only_empty)
    except Exception as e:
        logger.exception("批量同步 WMS 状态失败")
        raise HTTPException(500, f"批量同步失败: {e}")


# ──────────────────────────────────────────────
# 商品档案独立推送（2026-04-20 新增）
# 解决"单据过了但档案没过导致顺丰入不了库存"问题
# ──────────────────────────────────────────────

class ItemPushRequest(BaseModel):
    skus: list[str]
    source: str = "manual"


@router.post("/item-push")
async def item_push(req: ItemPushRequest):
    """批量推送商品档案到顺丰 ITEM_SERVICE（默认 -05 货主）"""
    if not req.skus:
        raise HTTPException(status_code=400, detail="skus 不能为空")
    if len(req.skus) > 2000:
        raise HTTPException(status_code=400, detail="单次最多 2000 个 SKU")
    return await sf_item_push.push_items(req.skus, source=req.source)


@router.get("/item-push/stats")
async def item_push_stats():
    """档案推送汇总"""
    return await sf_item_push.get_stats()


@router.get("/item-push/records")
async def item_push_records(
    status: str | None = None,
    keyword: str | None = None,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """档案推送历史"""
    rows = await sf_item_push.list_records(
        status=status, keyword=keyword, limit=limit, offset=offset
    )
    return {"items": rows, "count": len(rows)}


class SyncFirstCampRequest(BaseModel):
    dry_run: bool = False
    max_push: int | None = None
    source: str = "manual_first_camp"


@router.post("/sync-first-camp")
async def sync_first_camp(req: SyncFirstCampRequest):
    """把金蝶「首营已审批 + 物料已审核」的商品档案同步到顺丰。

    - dry_run=true：只返回差集统计，不实际推送
    - max_push：限制本次最多推多少个（测试保护）
    - source：落库 source 字段，方便区分手动/定时触发
    """
    try:
        return await sf_item_push.sync_first_camp_approved_to_sf(
            dry_run=req.dry_run,
            max_push=req.max_push,
            source=req.source,
        )
    except Exception as e:
        logger.exception("首营同步失败")
        raise HTTPException(status_code=500, detail=f"首营同步失败: {e}")
