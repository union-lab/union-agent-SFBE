"""顺丰供应链出库单服务

基于 SCC-PORTAL-V1.0 规范实现，支持：
- 多承运商选择（顺丰各产品 + 预留第三方）
- 出库单下发（SALE_ORDER_SERVICE）
- 出库单取消（CANCEL_SALE_ORDER_SERVICE）
- 出库单状态查询（SALE_ORDER_STATUS_QUERY_SERVICE）

承运商编码来源：SCC-PORTAL-V1.0 附录 4.2
"""

from __future__ import annotations

import hashlib
import base64
import ssl
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime

import httpx

from app.config import settings

logger = logging.getLogger("union")


# ── 承运商配置（SCC-PORTAL-V1.0 附录 4.2） ──

@dataclass
class CarrierProduct:
    code: str
    name: str
    description: str = ""


@dataclass
class Carrier:
    code: str
    name: str
    products: list[CarrierProduct] = field(default_factory=list)


CARRIERS: dict[str, Carrier] = {
    "CP": Carrier(
        code="CP",
        name="顺丰速运",
        products=[
            CarrierProduct("1", "标准快递", "顺丰标快，次日达"),
            CarrierProduct("2", "顺丰特惠", "经济件，2-3天"),
            CarrierProduct("SE0010", "电商特惠", "电商专用经济件"),
            CarrierProduct("SE0013", "电商速配", "电商专用速配件"),
            CarrierProduct("SE0053", "电商专配", "电商专配服务"),
            CarrierProduct("13", "物流普运", "大件/重货，零担物流"),
        ],
    ),
    "ZT": Carrier(
        code="ZT",
        name="自提",
        products=[
            CarrierProduct("ZT", "自提", "客户到仓自提，不产生运费"),
        ],
    ),
    # 以下为预留，编码需跟顺丰仓确认后启用
    # "YTO": Carrier(code="YTO", name="圆通速递", products=[...]),
    # "ZTO": Carrier(code="ZTO", name="中通快递", products=[...]),
    # "JD": Carrier(code="JD", name="京东物流", products=[...]),
}

PAYMENT_METHODS = {
    "1": "寄付（寄方付运费）",
    "2": "到付（收方付运费）",
    "3": "第三方付",
    "4": "寄付现结",
}

# 友联业务场景 → 承运商映射
SHIPPING_PRESETS: dict[str, dict] = {
    "sf_standard": {
        "name": "顺丰标快",
        "carrier": "CP",
        "carrier_product": "1",
        "payment": "1",
        "scenario": "电商/线下 → 顺丰标快",
    },
    "sf_economy": {
        "name": "顺丰特惠",
        "carrier": "CP",
        "carrier_product": "2",
        "payment": "1",
        "scenario": "电商 → 顺丰特惠",
    },
    "sf_ecommerce": {
        "name": "电商特惠",
        "carrier": "CP",
        "carrier_product": "SE0010",
        "payment": "1",
        "scenario": "电商 → 电商特惠件",
    },
    "sf_ecommerce_fast": {
        "name": "电商速配",
        "carrier": "CP",
        "carrier_product": "SE0013",
        "payment": "1",
        "scenario": "电商 → 电商速配",
    },
    "sf_heavy": {
        "name": "顺丰大件/重货",
        "carrier": "CP",
        "carrier_product": "13",
        "payment": "1",
        "scenario": "线下 → 顺丰大件/物流普运",
    },
    "sf_3rd_party_pay": {
        "name": "顺丰标快(第三方付)",
        "carrier": "CP",
        "carrier_product": "1",
        "payment": "3",
        "scenario": "电商 → 得物/京喜等平台承担运费",
    },
    "self_pickup": {
        "name": "客户自提",
        "carrier": "ZT",
        "carrier_product": "ZT",
        "payment": "1",
        "scenario": "线下 → 客户到顺丰仓自提",
    },
}


# ── SCC API 客户端 ──

def _make_sign(content: str) -> str:
    raw = content + settings.sf_special_str
    md5_hex = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return base64.b64encode(md5_hex.encode("utf-8")).decode("utf-8")


def _ssl_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _build_xml(service: str, body_xml: str) -> str:
    return (
        f'<Request service="{service}" lang="zh-CN">'
        f"<Head>"
        f"<AccessCode>{settings.sf_access_code}</AccessCode>"
        f"<Checkword>{settings.sf_checkword}</Checkword>"
        f"</Head>"
        f"<Body>{body_xml}</Body>"
        f"</Request>"
    )


def _parse_response(xml_text: str) -> dict:
    try:
        root = ET.fromstring(xml_text)
        head = root.findtext("Head", "UNKNOWN")
        error = root.find("Error")
        body = root.find("Body")

        result: dict = {"head": head, "raw": xml_text}
        if error is not None:
            result["error"] = error.text
            result["error_code"] = error.get("code")
        if body is not None:
            for child in body:
                for sub in child:
                    if sub.tag == "SaleOrders" or sub.tag == "SaleOrder":
                        result["orders"] = _parse_orders(sub)
                    else:
                        result[sub.tag] = sub.text
        return result
    except ET.ParseError:
        return {"head": "PARSE_ERROR", "raw": xml_text}


def _parse_orders(element: ET.Element) -> list[dict]:
    orders = []
    sale_orders = element.findall("SaleOrder") if element.tag == "SaleOrders" else [element]
    for so in sale_orders:
        order: dict = {}
        for child in so:
            order[child.tag] = child.text
        orders.append(order)
    return orders


async def _send_request(service: str, content: str) -> dict:
    sign = _make_sign(content)
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "sysSource": settings.sf_company_code,
        "serviceCode": service,
    }
    data = {"logistics_interface": content, "data_digest": sign}

    async with httpx.AsyncClient(timeout=30.0, verify=_ssl_ctx()) as client:
        resp = await client.post(settings.sf_base_url, headers=headers, data=data)

    return _parse_response(resp.text)


# ── 出库单构建 ──

@dataclass
class ReceiverInfo:
    name: str
    mobile: str
    province: str
    city: str
    area: str
    address: str
    company: str = ""
    zip_code: str = ""
    phone: str = ""
    email: str = ""


@dataclass
class SenderInfo:
    name: str = "友联医疗"
    mobile: str = ""
    company: str = "泉州市友联医疗器械有限公司"
    province: str = "福建省"
    city: str = "泉州市"
    area: str = "丰泽区"
    address: str = ""


@dataclass
class BuyerInfo:
    """客户信息（OrderBuyerInfo 节点）"""
    company: str = ""
    name: str = ""
    phone: str = ""
    province: str = ""
    city: str = ""
    address: str = ""
    zip_code: str = ""


@dataclass
class OrderItem:
    sku_no: str
    item_name: str = ""
    specification: str = ""
    quantity: int = 1
    unit: str = "个"
    price: float = 0.0
    lot: str = ""
    mfg_date: str = ""
    exp_date: str = ""
    note: str = ""
    inventory_status: str = "正品"
    is_present: str = ""
    vendor_code: str = ""
    brand: str = ""


@dataclass
class OutboundOrder:
    erp_order: str
    receiver: ReceiverInfo
    items: list[OrderItem]
    sale_org: str = ""
    buyer: BuyerInfo | None = None
    customer_id: str = ""
    carrier_code: str = "CP"
    carrier_product: str = "1"
    payment_of_charge: str = "1"
    waybill_no: str = ""
    monthly_account: str = ""
    erp_order_type: str = "10"
    sf_order_type: str = "10"
    order_date: str = ""
    note: str = ""
    remark: str = ""
    sender: SenderInfo | None = None
    warehouse_code: str = ""
    company_code: str = ""
    complete_delivery: str = ""
    trade_platform: str = ""
    trade_order: str = ""
    shop_name: str = ""

    @classmethod
    def from_preset(
        cls,
        preset_key: str,
        erp_order: str,
        receiver: ReceiverInfo,
        items: list[OrderItem],
        **kwargs,
    ) -> "OutboundOrder":
        preset = SHIPPING_PRESETS.get(preset_key)
        if not preset:
            raise ValueError(f"未知预设: {preset_key}，可用: {list(SHIPPING_PRESETS.keys())}")
        return cls(
            erp_order=erp_order,
            receiver=receiver,
            items=items,
            carrier_code=preset["carrier"],
            carrier_product=preset["carrier_product"],
            payment_of_charge=preset["payment"],
            **kwargs,
        )


def _xml_tag(tag: str, value: str) -> str:
    if not value:
        return ""
    return f"<{tag}>{value}</{tag}>"


def build_sale_order_xml(order: OutboundOrder) -> str:
    """根据 SCC-PORTAL-V1.0 §3.11 构建出库单 XML"""
    company = order.company_code or settings.sf_company_code
    warehouse = order.warehouse_code or settings.sf_warehouse_code
    order_date = order.order_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    items_xml = ""
    for i, item in enumerate(order.items, 1):
        items_xml += (
            "<OrderItem>"
            f"<ErpOrderLineNum>{i}</ErpOrderLineNum>"
            f"<SkuNo>{item.sku_no}</SkuNo>"
            + _xml_tag("ItemName", item.item_name)
            + _xml_tag("ItemSpecifications", item.specification)
            + _xml_tag("ItemUom", item.unit)
            + f"<ItemQuantity>{item.quantity}</ItemQuantity>"
            + _xml_tag("ItemPrice", str(item.price) if item.price else "")
            + _xml_tag("ItemBrand", item.brand)
            + _xml_tag("Lot", item.lot)
            + _xml_tag("MfgDate", item.mfg_date)
            + _xml_tag("ExpDate", item.exp_date)
            + f"<InventoryStatus>{item.inventory_status}</InventoryStatus>"
            + _xml_tag("IsPresent", item.is_present)
            + _xml_tag("VendorCode", item.vendor_code)
            + _xml_tag("Note", item.note)
            + "</OrderItem>"
        )

    r = order.receiver
    # 顺丰面单只打印 ReceiverAddress 字段，为避免收货地址缺省/市/区，
    # 若 address 不包含省市区则拼接完整地址（省/市/区仍单独传供分拣路由）
    full_addr = r.address or ""
    prefix_parts = [p for p in (r.province, r.city, r.area) if p]
    if prefix_parts and not any(p and p in full_addr for p in prefix_parts):
        full_addr = "".join(prefix_parts) + full_addr
    receiver_xml = (
        "<OrderReceiverInfo>"
        + _xml_tag("ReceiverCompany", r.company)
        + f"<ReceiverName>{r.name}</ReceiverName>"
        + _xml_tag("ReceiverEmail", r.email)
        + _xml_tag("ReceiverZipCode", r.zip_code)
        + f"<ReceiverMobile>{r.mobile}</ReceiverMobile>"
        + _xml_tag("ReceiverPhone", r.phone or r.mobile)
        + "<ReceiverCountry>中国</ReceiverCountry>"
        + f"<ReceiverProvince>{r.province}</ReceiverProvince>"
        + f"<ReceiverCity>{r.city}</ReceiverCity>"
        + f"<ReceiverArea>{r.area}</ReceiverArea>"
        + f"<ReceiverAddress>{full_addr}</ReceiverAddress>"
        + "</OrderReceiverInfo>"
    )

    sender_xml = ""
    if order.sender:
        s = order.sender
        sender_xml = (
            "<OrderSenderInfo>"
            + _xml_tag("SenderCompany", s.company)
            + _xml_tag("SenderName", s.name)
            + _xml_tag("SenderMobile", s.mobile)
            + f"<SenderProvince>{s.province}</SenderProvince>"
            + f"<SenderCity>{s.city}</SenderCity>"
            + _xml_tag("SenderArea", s.area)
            + _xml_tag("SenderAddress", s.address)
            + "</OrderSenderInfo>"
        )

    carrier_xml = (
        "<OrderCarrier>"
        + f"<Carrier>{order.carrier_code}</Carrier>"
        + f"<CarrierProduct>{order.carrier_product}</CarrierProduct>"
        + _xml_tag("WaybillNo", order.waybill_no)
        + f"<PaymentOfCharge>{order.payment_of_charge}</PaymentOfCharge>"
        + _xml_tag("MonthlyAccount", order.monthly_account)
        + "</OrderCarrier>"
    )

    buyer_xml = ""
    if order.buyer:
        b = order.buyer
        buyer_xml = (
            "<OrderBuyerInfo>"
            + _xml_tag("BuyerCompany", b.company)
            + _xml_tag("BuyerName", b.name)
            + _xml_tag("BuyerPhone", b.phone)
            + _xml_tag("BuyerProvince", b.province)
            + _xml_tag("BuyerCity", b.city)
            + _xml_tag("BuyerAddress", b.address)
            + _xml_tag("BuyerZipCode", b.zip_code)
            + "</OrderBuyerInfo>"
        )

    body_xml = (
        "<SaleOrderRequest>"
        f"<CompanyCode>{company}</CompanyCode>"
        "<SaleOrders>"
        "<SaleOrder>"
        f"<CompanyCode>{company}</CompanyCode>"
        f"<WarehouseCode>{warehouse}</WarehouseCode>"
        f"<ErpOrder>{order.erp_order}</ErpOrder>"
        f"<ErpOrderType>{order.erp_order_type}</ErpOrderType>"
        f"<SFOrderType>{order.sf_order_type}</SFOrderType>"
        f"<OrderDate>{order_date}</OrderDate>"
        + _xml_tag("OrderNote", order.note)
        + _xml_tag("Remark", order.remark)
        + _xml_tag("CustomerId", order.customer_id)
        + _xml_tag("CompanyNote", order.sale_org)
        + _xml_tag("ShopName", order.shop_name)
        + _xml_tag("TradePlatform", order.trade_platform)
        + _xml_tag("TradeOrder", order.trade_order)
        + _xml_tag("CompleteDelivery", order.complete_delivery)
        + carrier_xml
        + receiver_xml
        + sender_xml
        + buyer_xml
        + f"<OrderItems>{items_xml}</OrderItems>"
        + "</SaleOrder>"
        "</SaleOrders>"
        "</SaleOrderRequest>"
    )

    return _build_xml("SALE_ORDER_SERVICE", body_xml)


# ── 核心操作 ──

async def push_outbound_order(order: OutboundOrder) -> dict:
    """下发出库单到顺丰仓"""
    content = build_sale_order_xml(order)

    carrier_name = CARRIERS.get(order.carrier_code, Carrier(order.carrier_code, "未知"))
    logger.info(
        f"顺丰出库: {order.erp_order} | "
        f"承运商={carrier_name.name}({order.carrier_code}) | "
        f"产品={order.carrier_product} | "
        f"运费={PAYMENT_METHODS.get(order.payment_of_charge, '未知')} | "
        f"商品数={len(order.items)}"
    )

    result = await _send_request("SALE_ORDER_SERVICE", content)

    if result["head"] == "OK":
        logger.info(f"顺丰出库成功: {order.erp_order}")
    else:
        logger.warning(
            f"顺丰出库失败: {order.erp_order} | "
            f"head={result['head']} | error={result.get('error', '')}"
        )

    return result


async def cancel_outbound_order(
    erp_order: str,
    shipment_id: str = "",
    remark: str = "",
) -> dict:
    """取消出库单（仓库包装状态之前允许取消）

    参数:
      erp_order: 金蝶发货通知单号（FHTZ）
      shipment_id: 顺丰出库单号（OutboundOrderId），可空
      remark: 取消原因（CancelSaleOrderRequest.Remark，可选 256 字内），
              用于在顺丰侧留痕，例如"金蝶反审核自动撤回"
    """
    head_extra = _xml_tag("Remark", remark) if remark else ""
    cancel_xml = (
        "<CancelSaleOrderRequest>"
        f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
        + head_extra
        + "<SaleOrders>"
        "<SaleOrder>"
        f"<ErpOrder>{erp_order}</ErpOrder>"
        + _xml_tag("OutboundOrderId", shipment_id)
        + "</SaleOrder>"
        "</SaleOrders>"
        "</CancelSaleOrderRequest>"
    )
    content = _build_xml("CANCEL_SALE_ORDER_SERVICE", cancel_xml)

    logger.info(f"顺丰取消出库: {erp_order} remark={remark!r}")
    return await _send_request("CANCEL_SALE_ORDER_SERVICE", content)


async def cancel_inbound_order(erp_order: str) -> dict:
    """取消入库单（仓库未开始收货时可取消，已收货或不存在均会失败）

    SCC-PORTAL-V1.0 §3.8 CANCEL_PURCHASE_ORDER_SERVICE，
    报文与出库取消对称（静茹 2026-04-28 微信确认）。

    参数:
      erp_order: 金蝶单号（直接调拨/分布式调入/采购入库的 FBillNo）
    """
    cancel_xml = (
        "<CancelPurchaseOrderRequest>"
        f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
        "<PurchaseOrders>"
        "<PurchaseOrder>"
        f"<ErpOrder>{erp_order}</ErpOrder>"
        "</PurchaseOrder>"
        "</PurchaseOrders>"
        "</CancelPurchaseOrderRequest>"
    )
    content = _build_xml("CANCEL_PURCHASE_ORDER_SERVICE", cancel_xml)

    logger.info(f"顺丰取消入库: {erp_order}")
    return await _send_request("CANCEL_PURCHASE_ORDER_SERVICE", content)


async def query_outbound_status(erp_order: str) -> dict:
    """查询出库单状态"""
    query_xml = (
        "<SaleOrderStatusQueryRequest>"
        f"<CompanyCode>{settings.sf_company_code}</CompanyCode>"
        "<SaleOrders>"
        "<SaleOrder>"
        f"<ErpOrder>{erp_order}</ErpOrder>"
        "</SaleOrder>"
        "</SaleOrders>"
        "</SaleOrderStatusQueryRequest>"
    )
    content = _build_xml("SALE_ORDER_STATUS_QUERY_SERVICE", query_xml)

    logger.info(f"顺丰查询出库状态: {erp_order}")
    return await _send_request("SALE_ORDER_STATUS_QUERY_SERVICE", content)
