"""从顺丰响应提取可读错误，仅用于展示，不改变推送或重试行为。"""

import xml.etree.ElementTree as ET


def response_error(raw, order_keys=()):
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return None
    # 兼容带命名空间的顺丰报文。
    for node in root.iter():
        node.tag = node.tag.rsplit("}", 1)[-1]
    head = (root.findtext("Head") or "").strip()
    keys = {str(k) for k in order_keys if k}
    messages = []
    for order in root.iter():
        if order.tag not in {"SaleOrder", "PurchaseOrder"}:
            continue
        if keys and (order.findtext("ErpOrder") or "").strip() not in keys:
            continue
        result = (order.findtext("Result") or "").strip()
        if result == "1" or (not result and head == "OK"):
            continue
        note = (order.findtext("Note") or "").strip()
        if note and note not in messages:
            messages.append(note)
    for error in root.findall("./Error"):
        note = (error.text or "").strip()
        code = (error.get("code") or "").strip()
        message = f"{note}（错误码：{code}）" if note and code else note
        if not message and code:
            message = f"顺丰错误码：{code}"
        if message and message not in messages:
            messages.append(message)
    return "；".join(messages)[:2000] or None


def fill_push_error(record):
    """历史失败单未存 error_message 时，从已有原始响应补齐展示。"""
    if record.get("status") != "failed" or (record.get("error_message") or "").strip():
        return
    record["error_message"] = response_error(
        record.get("sf_response"), (record.get("bill_no"), record.get("erp_order"))
    )
