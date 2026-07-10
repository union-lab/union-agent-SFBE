"""顺丰供应链 WMS 推送回调接口

顺丰仓库完成入库/出库等操作后，主动推送数据到此接口。
支持 XML 和 JSON 两种报文格式，自动识别并解析。

推送接口清单：
- PURCHASE_ORDER_INBOUND_PUSH_SERVICE  入库单明细推送
- SALE_ORDER_OUTBOUND_DETAIL_PUSH_SERVICE  出库单明细推送
- SALE_ORDER_STATUS_PUSH_SERVICE  出库单状态推送
- INVENTORY_CHANGE_SERVICE  库存变化推送
- TRANSPORT_TRACE_PUSH_SERVICE  运输轨迹推送
"""

from __future__ import annotations

import hashlib
import base64
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Request, Response

from app.config import settings
from app.db.pool import get_pool

router = APIRouter(prefix="/api/sf", tags=["顺丰供应链"])
logger = logging.getLogger("union")
CST = timezone(timedelta(hours=8))


def verify_sign(content: str, digest: str) -> bool:
    """验证顺丰推送签名：content + 特殊串 → MD5 hex → Base64"""
    expected = hashlib.md5(
        (content + settings.sf_special_str).encode("utf-8")
    ).hexdigest()
    expected_b64 = base64.b64encode(expected.encode("utf-8")).decode("utf-8")
    return digest == expected_b64


def xml_to_dict(element: ET.Element) -> dict | str:
    """递归将 XML Element 转为 dict"""
    children = list(element)
    if not children:
        return element.text or ""
    result: dict = {}
    for child in children:
        key = child.tag
        value = xml_to_dict(child)
        if key in result:
            existing = result[key]
            if isinstance(existing, list):
                existing.append(value)
            else:
                result[key] = [existing, value]
        else:
            result[key] = value
    return result


def parse_xml_payload(content: str) -> tuple[str, dict]:
    """解析 XML 推送报文，返回 (service_code, body_dict)"""
    root = ET.fromstring(content)
    service_code = root.attrib.get("service", "UNKNOWN")
    body_el = root.find("Body")
    body_dict = xml_to_dict(body_el) if body_el is not None else {}
    return service_code, body_dict


def parse_json_payload(content: str) -> tuple[str, dict]:
    """解析 JSON 推送报文，返回 (service_code, body_dict)"""
    data = json.loads(content)
    service_code = data.get("ServiceCode") or data.get("service") or "UNKNOWN"
    body = data.get("Body") or data
    return service_code, body


def build_xml_response(service_code: str, success: bool, note: str = "") -> str:
    """构建 XML 格式的响应报文"""
    head = "OK" if success else "ERR"
    if success:
        tag = _response_tag(service_code)
        return (
            f'<Response service="{service_code}">'
            f"<Head>{head}</Head>"
            f"<Body><{tag}>"
            f"<Result>1</Result>"
            f"<Note>{note or '处理成功'}</Note>"
            f"</{tag}></Body>"
            f"</Response>"
        )
    return (
        f'<Response service="{service_code}">'
        f"<Head>ERR</Head>"
        f'<Error code="99999">{note or "处理失败"}</Error>'
        f"</Response>"
    )


def _response_tag(service_code: str) -> str:
    """根据 service_code 推导响应 Body 的标签名"""
    mapping = {
        "PURCHASE_ORDER_INBOUND_PUSH_SERVICE": "PurchaseOrderInboundResponse",
        "SALE_ORDER_OUTBOUND_DETAIL_PUSH_SERVICE": "SaleOrderOutboundDetailResponse",
        "SALE_ORDER_STATUS_PUSH_SERVICE": "SaleOrderStatusResponse",
        "INVENTORY_CHANGE_SERVICE": "InventoryChangeResponse",
        "TRANSPORT_TRACE_PUSH_SERVICE": "TransportTracePushResponse",
        "RT_INVENTORY_PUSH_SERVICE": "RtInventoryPushResponse",
        "INVENTORY_MOVE_PUSH_SERVICE": "InventoryMovePushResponse",
        "SALE_ORDER_HANDOVER_PUSH_SERVICE": "SaleOrderHandoverPushResponse",
        "ITEM_CHANGE_PUSH_SERVICE": "ItemChangePushResponse",
        "CYCLE_COUNT_REQUEST_PUSH_SERVICE": "CycleCountRequestPushResponse",
    }
    return mapping.get(service_code, "Response")


def extract_order_info(service_code: str, body: dict) -> tuple[str | None, str | None, str | None]:
    """从推送 body 中提取关键字段：transaction_id, erp_order, warehouse_code"""
    transaction_id = body.get("TransactionId")

    # 入库推送
    if "PurchaseOrders" in body:
        orders = body["PurchaseOrders"]
        if isinstance(orders, list) and orders:
            first = orders[0]
        elif isinstance(orders, dict) and "PurchaseOrder" in orders:
            first = orders["PurchaseOrder"]
            if isinstance(first, list):
                first = first[0]
        else:
            first = {}
        return transaction_id, first.get("ErpOrder"), first.get("WarehouseCode")

    # 出库推送
    if "SaleOrders" in body:
        orders = body["SaleOrders"]
        if isinstance(orders, list) and orders:
            first = orders[0]
        elif isinstance(orders, dict) and "SaleOrder" in orders:
            first = orders["SaleOrder"]
            if isinstance(first, list):
                first = first[0]
        else:
            first = {}
        return transaction_id, first.get("ErpOrder"), first.get("WarehouseCode")

    # 出库状态推送
    if "SaleOrderStatus" in body:
        status = body["SaleOrderStatus"]
        if isinstance(status, list) and status:
            first = status[0]
        elif isinstance(status, dict):
            first = status
        else:
            first = {}
        return transaction_id, first.get("ErpOrder"), first.get("WarehouseCode")

    # 通用：直接从 body 取
    return (
        transaction_id,
        body.get("ErpOrder") or body.get("ErpOrderNo"),
        body.get("WarehouseCode"),
    )


@router.post("/callback")
async def sf_push_callback(request: Request):
    """顺丰 WMS 推送回调统一入口

    支持两种推送格式：
    1. application/x-www-form-urlencoded：logistics_interface=<报文>&data_digest=<签名>
    2. application/json：直接 POST JSON body
    """
    content_type = request.headers.get("content-type", "")
    content = ""
    digest = ""

    if "application/json" in content_type:
        raw = await request.body()
        content = raw.decode("utf-8") if raw else ""
        digest = request.headers.get("data-digest", "")
    else:
        form = await request.form()
        content = form.get("logistics_interface", "")
        digest = form.get("data_digest", "")

    if not content:
        logger.warning("顺丰推送: 空报文")
        return Response(
            content=build_xml_response("UNKNOWN", False, "空报文"),
            media_type="application/xml",
        )

    content_str = str(content)
    digest_str = str(digest)

    if digest_str and not verify_sign(content_str, digest_str):
        logger.warning("顺丰推送: 签名验证失败")
        return Response(
            content=build_xml_response("UNKNOWN", False, "签名验证失败"),
            media_type="application/xml",
        )

    # 自动识别 XML / JSON
    is_xml = content_str.strip().startswith("<")
    try:
        if is_xml:
            service_code, body = parse_xml_payload(content_str)
            fmt = "xml"
        else:
            service_code, body = parse_json_payload(content_str)
            fmt = "json"
    except Exception as e:
        logger.error(f"顺丰推送: 报文解析失败 - {e}")
        return Response(
            content=build_xml_response("UNKNOWN", False, f"报文解析失败: {e}"),
            media_type="application/xml",
        )

    company_code = body.get("CompanyCode", settings.sf_company_code)
    transaction_id, erp_order, warehouse_code = extract_order_info(service_code, body)

    logger.info(
        f"顺丰推送: {service_code} | 订单={erp_order} | 仓库={warehouse_code} | 格式={fmt}"
    )

    # 存入数据库
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ods_sf_push_log
                    (service_code, transaction_id, company_code, warehouse_code,
                     erp_order, raw_payload, format, status)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, 'received')
                """,
                service_code,
                transaction_id,
                company_code,
                warehouse_code,
                erp_order,
                json.dumps(body, ensure_ascii=False, default=str),
                fmt,
            )
    except Exception as e:
        logger.error(f"顺丰推送: 存储失败 - {e}", exc_info=True)
        return Response(
            content=build_xml_response(service_code, False, "存储失败"),
            media_type="application/xml",
        )

    return Response(
        content=build_xml_response(service_code, True, "接收成功"),
        media_type="application/xml",
    )


@router.get("/push-logs")
async def list_push_logs(
    page: int = 1,
    page_size: int = 20,
    service_code: str | None = None,
    erp_order: str | None = None,
):
    """查询顺丰推送日志"""
    pool = await get_pool()
    conditions: list[str] = []
    params: list = []
    idx = 1

    if service_code:
        conditions.append(f"service_code = ${idx}")
        params.append(service_code)
        idx += 1
    if erp_order:
        conditions.append(f"erp_order ILIKE ${idx}")
        params.append(f"%{erp_order}%")
        idx += 1

    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""

    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT count(*) FROM ods_sf_push_log{where}", *params)
        rows = await conn.fetch(
            f"SELECT * FROM ods_sf_push_log{where} ORDER BY received_at DESC "
            f"LIMIT ${idx} OFFSET ${idx + 1}",
            *params,
            page_size,
            (page - 1) * page_size,
        )

    from app.db.pool import row_to_dict
    return {
        "items": [row_to_dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
