"""入库展示只依据顺丰实际收货回传，不把接口接单成功当成入库。"""

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

INBOUND = {"purchase_in", "transfer", "transfer_in", "manual_inbound"}
SERVICE = "PURCHASE_ORDER_INBOUND_PUSH_SERVICE"


def decoded(value, fallback):
    try:
        return json.loads(value) if isinstance(value, str) else (value or fallback)
    except (TypeError, ValueError):
        return fallback


def number(value):
    try:
        n = Decimal(str(value))
        return n if n.is_finite() and n >= 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def receipt_time(value):
    try:
        d = datetime.fromisoformat(str(value))
        return (
            d.replace(tzinfo=timezone(timedelta(hours=8))).isoformat()
            if d.tzinfo is None
            else d.isoformat()
        )
    except (TypeError, ValueError):
        return None


def summarize(record, callback=None):
    if record["bill_type"] not in INBOUND:
        return None
    result = {
        "label": "待入库确认",
        "state": "unknown",
        "received_at": None,
        "items": [],
        "quantities": [],
        "note": "尚无顺丰实收回传，不能据此判断未入库",
    }
    if not callback:
        return result
    payload = decoded(callback["raw_payload"], {})
    orders = payload.get("PurchaseOrders", []) if isinstance(payload, dict) else []
    if isinstance(orders, dict):
        orders = [orders]
    keys = {record["bill_no"], record.get("erp_order")}
    orders = [o for o in orders if isinstance(o, dict) and o.get("ErpOrder") in keys]
    if len(orders) != 1:
        return result
    planned = defaultdict(Decimal)
    names, units = {}, {}
    for item in decoded(record.get("items_json"), []):
        key = (str(item.get("sku", "")), str(item.get("lot", "")))
        qty = number(item.get("qty"))
        if qty is None:
            return result
        planned[key] += qty
        names[key], units[key] = item.get("name", key[0]), item.get("unit", "")
    actual, rejected = defaultdict(Decimal), defaultdict(Decimal)
    times = defaultdict(list)
    invalid = False
    received_items = orders[0].get("Items")
    if not isinstance(received_items, list) or not received_items:
        return result
    for item in received_items:
        if not isinstance(item, dict):
            invalid = True
            continue
        key = (str(item.get("SkuNo", "")), str(item.get("Lot", "")))
        qty, reject = number(item.get("ActualQty")), number(item.get("RejectionQty", 0))
        if qty is None or reject is None:
            invalid = True
            continue
        actual[key] += qty
        rejected[key] += reject
        t = receipt_time(item.get("ReceiptTime"))
        if t and qty > 0:
            times[key].append(t)
        names.setdefault(key, item.get("SkuName", key[0]))
    if invalid:
        result.update(
            label="入库待核实",
            state="exception",
            note="顺丰回传数量缺失或格式异常，实收数量待核实",
        )
        return result
    totals = defaultdict(lambda: [Decimal(0), Decimal(0), Decimal(0)])
    for key in sorted(planned.keys() | actual.keys()):
        unit = units.get(key, "")
        values = [
            planned.get(key, Decimal(0)),
            actual.get(key, Decimal(0)),
            rejected.get(key, Decimal(0)),
        ]
        totals[unit] = [a + b for a, b in zip(totals[unit], values)]
        result["items"].append(
            dict(
                sku=key[0],
                lot=key[1],
                name=names[key],
                unit=unit,
                planned=float(values[0]),
                actual=float(values[1]),
                rejected=float(values[2]),
                received_at=max(times[key], default=None),
            )
        )
    result["quantities"] = [
        dict(unit=u, planned=float(v[0]), actual=float(v[1]), rejected=float(v[2]))
        for u, v in totals.items()
    ]
    result["received_at"] = max((t for ts in times.values() for t in ts), default=None)
    result["note"] = "依据最新一笔顺丰入库回传；多次回传不累加，入库时间取实际收货时间"
    if (
        invalid
        or not planned
        or any(k not in planned or q > planned[k] for k, q in actual.items())
    ):
        result.update(label="入库待核实", state="exception")
    elif sum(actual.values()) > 0:
        complete = all(actual[k] == qty for k, qty in planned.items()) and not sum(
            rejected.values()
        )
        result.update(
            label="已入库" if complete else "部分入库",
            state="complete" if complete else "partial",
        )
    else:
        result.update(label="未入库", state="empty")
    return result


async def load_summaries(conn, records):
    inbound = [dict(r) for r in records if r["bill_type"] in INBOUND]
    keys = list(
        {str(r[k]) for r in inbound for k in ("bill_no", "erp_order") if r.get(k)}
    )
    if not keys:
        return {}
    callbacks = await conn.fetch(
        """SELECT DISTINCT ON (erp_order) erp_order,raw_payload,received_at,id
           FROM ods_sf_push_log WHERE erp_order=ANY($1::text[]) AND service_code=$2
           ORDER BY erp_order,received_at DESC,id DESC""",
        keys,
        SERVICE,
    )
    by_key = {r["erp_order"]: r for r in callbacks}
    result = {}
    for r in inbound:
        matches = [by_key[k] for k in (r["bill_no"], r.get("erp_order")) if k in by_key]
        latest = max(matches, key=lambda c: (c["received_at"], c["id"]), default=None)
        result[r["id"]] = summarize(r, latest)
    return result
