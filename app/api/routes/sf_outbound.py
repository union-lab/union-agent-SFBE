"""顺丰出库单管理 API

提供出库单下发、取消、状态查询，以及承运商/预设方案查询。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.config import settings
from app.db.pool import get_pool
from app.services.sf_outbound import (
    CARRIERS,
    SHIPPING_PRESETS,
    PAYMENT_METHODS,
    OutboundOrder,
    ReceiverInfo,
    SenderInfo,
    BuyerInfo,
    OrderItem,
    push_outbound_order,
    cancel_outbound_order,
    query_outbound_status,
)

router = APIRouter(prefix="/api/sf/outbound", tags=["顺丰出库"])
logger = logging.getLogger("union")
CST = timezone(timedelta(hours=8))


# ── 请求模型 ──

class ReceiverInput(BaseModel):
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


class SenderInput(BaseModel):
    name: str = "友联医疗"
    mobile: str = ""
    company: str = "泉州市友联医疗器械有限公司"
    province: str = "福建省"
    city: str = "泉州市"
    area: str = "丰泽区"
    address: str = ""


class BuyerInput(BaseModel):
    company: str = Field("", description="客户公司名")
    name: str = Field("", description="客户联系人")
    phone: str = Field("", description="客户电话")
    province: str = ""
    city: str = ""
    address: str = ""
    zip_code: str = ""


class OrderItemInput(BaseModel):
    sku_no: str = Field(..., description="商品编码")
    item_name: str = Field("", description="商品名称")
    specification: str = Field("", description="规格型号")
    quantity: int = Field(1, description="数量")
    unit: str = Field("个", description="单位")
    price: float = Field(0.0, description="单价")
    lot: str = Field("", description="批号")
    mfg_date: str = Field("", description="生产日期（YYYY-MM-DD）")
    exp_date: str = Field("", description="有效期（YYYY-MM-DD）")
    note: str = Field("", description="行备注")
    inventory_status: str = "正品"
    is_present: str = ""
    vendor_code: str = Field("", description="供应商编码")
    brand: str = Field("", description="品牌")


class PushOrderRequest(BaseModel):
    erp_order: str = Field(..., description="ERP 订单号，唯一标识")
    sale_org: str = Field("", description="发货主体（金蝶销售组织名称，如 友联/优益诺/小猴快跑）")
    buyer: BuyerInput | None = Field(None, description="客户信息")
    customer_id: str = Field("", description="客户编码（金蝶客户编码）")
    receiver: ReceiverInput
    items: list[OrderItemInput] = Field(..., min_length=1)
    preset: str | None = Field(
        None,
        description="发货预设方案，优先级高于 carrier_code/carrier_product。可选值见 GET /presets",
    )
    carrier_code: str = Field("CP", description="承运商编码，默认顺丰")
    carrier_product: str = Field("1", description="承运商产品编码，默认标准快递")
    payment_of_charge: str = Field("1", description="运费支付方式：1寄付/2到付/3第三方付")
    waybill_no: str = Field("", description="运单号（已有运单时传入）")
    monthly_account: str = Field("", description="月结账号")
    order_date: str = Field("", description="订单日期，默认当前")
    note: str = Field("", description="订单备注")
    remark: str = Field("", description="订单附言")
    sender: SenderInput | None = None
    trade_platform: str = Field("", description="交易平台")
    trade_order: str = Field("", description="平台订单号")
    shop_name: str = Field("", description="店铺名")
    complete_delivery: str = Field("", description="是否整单发货 Y/N")


class CancelOrderRequest(BaseModel):
    erp_order: str
    shipment_id: str = ""


# ── 端点 ──

@router.get("/carriers")
async def list_carriers():
    """查询所有支持的承运商及其产品"""
    result = []
    for c in CARRIERS.values():
        result.append({
            "code": c.code,
            "name": c.name,
            "products": [
                {"code": p.code, "name": p.name, "description": p.description}
                for p in c.products
            ],
        })
    return {"carriers": result}


@router.get("/presets")
async def list_presets():
    """查询所有发货预设方案（友联业务场景快捷选择）"""
    result = []
    for key, preset in SHIPPING_PRESETS.items():
        carrier = CARRIERS.get(preset["carrier"])
        result.append({
            "key": key,
            "name": preset["name"],
            "carrier_code": preset["carrier"],
            "carrier_name": carrier.name if carrier else preset["carrier"],
            "carrier_product": preset["carrier_product"],
            "payment": preset["payment"],
            "payment_desc": PAYMENT_METHODS.get(preset["payment"], ""),
            "scenario": preset["scenario"],
        })
    return {"presets": result}


@router.post("/push")
async def push_order(req: PushOrderRequest):
    """下发出库单到顺丰仓

    两种使用方式：
    1. 传 preset 字段 → 自动填充承运商/产品/支付方式
    2. 手动传 carrier_code + carrier_product + payment_of_charge
    """
    receiver = ReceiverInfo(
        name=req.receiver.name,
        mobile=req.receiver.mobile,
        province=req.receiver.province,
        city=req.receiver.city,
        area=req.receiver.area,
        address=req.receiver.address,
        company=req.receiver.company,
        zip_code=req.receiver.zip_code,
        phone=req.receiver.phone,
        email=req.receiver.email,
    )

    items = [
        OrderItem(
            sku_no=it.sku_no,
            item_name=it.item_name,
            specification=it.specification,
            quantity=it.quantity,
            unit=it.unit,
            price=it.price,
            lot=it.lot,
            mfg_date=it.mfg_date,
            exp_date=it.exp_date,
            note=it.note,
            inventory_status=it.inventory_status,
            is_present=it.is_present,
            vendor_code=it.vendor_code,
            brand=it.brand,
        )
        for it in req.items
    ]

    sender = None
    if req.sender:
        sender = SenderInfo(
            name=req.sender.name,
            mobile=req.sender.mobile,
            company=req.sender.company,
            province=req.sender.province,
            city=req.sender.city,
            area=req.sender.area,
            address=req.sender.address,
        )

    buyer = None
    if req.buyer:
        buyer = BuyerInfo(
            company=req.buyer.company,
            name=req.buyer.name,
            phone=req.buyer.phone,
            province=req.buyer.province,
            city=req.buyer.city,
            address=req.buyer.address,
            zip_code=req.buyer.zip_code,
        )

    extra_kwargs: dict = {}
    if req.sale_org:
        extra_kwargs["sale_org"] = req.sale_org
    if buyer:
        extra_kwargs["buyer"] = buyer
    if req.customer_id:
        extra_kwargs["customer_id"] = req.customer_id
    if req.waybill_no:
        extra_kwargs["waybill_no"] = req.waybill_no
    if req.monthly_account:
        extra_kwargs["monthly_account"] = req.monthly_account
    if req.order_date:
        extra_kwargs["order_date"] = req.order_date
    if req.note:
        extra_kwargs["note"] = req.note
    if req.remark:
        extra_kwargs["remark"] = req.remark
    if sender:
        extra_kwargs["sender"] = sender
    if req.trade_platform:
        extra_kwargs["trade_platform"] = req.trade_platform
    if req.trade_order:
        extra_kwargs["trade_order"] = req.trade_order
    if req.shop_name:
        extra_kwargs["shop_name"] = req.shop_name
    if req.complete_delivery:
        extra_kwargs["complete_delivery"] = req.complete_delivery

    if req.preset:
        order = OutboundOrder.from_preset(
            preset_key=req.preset,
            erp_order=req.erp_order,
            receiver=receiver,
            items=items,
            **extra_kwargs,
        )
    else:
        order = OutboundOrder(
            erp_order=req.erp_order,
            receiver=receiver,
            items=items,
            carrier_code=req.carrier_code,
            carrier_product=req.carrier_product,
            payment_of_charge=req.payment_of_charge,
            **extra_kwargs,
        )

    result = await push_outbound_order(order)

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ods_sf_push_log
                (service_code, company_code, warehouse_code, erp_order,
                 raw_payload, format, status)
            VALUES ('SALE_ORDER_SERVICE', $1, $2, $3, $4::jsonb, 'xml', $5)
            """,
            order.company_code or settings.sf_company_code,
            order.warehouse_code or settings.sf_warehouse_code,
            order.erp_order,
            json.dumps({
                "carrier": order.carrier_code,
                "carrier_product": order.carrier_product,
                "payment": order.payment_of_charge,
                "preset": req.preset,
                "items_count": len(order.items),
                "result_head": result.get("head"),
                "result_error": result.get("error"),
            }, ensure_ascii=False),
            "success" if result["head"] == "OK" else "failed",
        )

    if result["head"] != "OK":
        raise HTTPException(
            status_code=502,
            detail={
                "message": f"顺丰仓拒绝: {result.get('error', '未知错误')}",
                "error_code": result.get("error_code"),
                "raw": result.get("raw", "")[:500],
            },
        )

    return {
        "success": True,
        "erp_order": order.erp_order,
        "carrier": order.carrier_code,
        "carrier_product": order.carrier_product,
        "payment": PAYMENT_METHODS.get(order.payment_of_charge, order.payment_of_charge),
        "result": {k: v for k, v in result.items() if k != "raw"},
    }


@router.post("/cancel")
async def cancel_order(req: CancelOrderRequest):
    """取消出库单（仓库包装前可取消）"""
    result = await cancel_outbound_order(req.erp_order, req.shipment_id)

    if result["head"] != "OK":
        raise HTTPException(
            status_code=502,
            detail={
                "message": f"取消失败: {result.get('error', '未知错误')}",
                "raw": result.get("raw", "")[:500],
            },
        )

    return {"success": True, "erp_order": req.erp_order}


@router.get("/status/{erp_order}")
async def get_order_status(erp_order: str):
    """查询出库单在顺丰仓的处理状态"""
    result = await query_outbound_status(erp_order)

    if result["head"] != "OK":
        raise HTTPException(
            status_code=502,
            detail={
                "message": f"查询失败: {result.get('error', '未知错误')}",
                "raw": result.get("raw", "")[:500],
            },
        )

    return {
        "erp_order": erp_order,
        "orders": result.get("orders", []),
        "result": {k: v for k, v in result.items() if k != "raw"},
    }
