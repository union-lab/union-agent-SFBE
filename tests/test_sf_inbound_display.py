from app.services.sf_inbound_display import summarize


def rec(items=None, **kw):
    return dict(
        bill_no="P1",
        erp_order="P1",
        bill_type="purchase_in",
        items_json=items
        or [
            {"sku": "A", "lot": "L", "qty": 100, "name": "浮针", "unit": "支"},
            {"sku": "B", "lot": "M", "qty": 1, "unit": "支"},
        ],
        **kw,
    )


def cb(qty=100, other=1, **kw):
    return {
        "raw_payload": {
            "PurchaseOrders": [
                {
                    "ErpOrder": "P1",
                    "Items": [
                        dict(
                            SkuNo="A",
                            Lot="L",
                            ActualQty=qty,
                            ReceiptTime="2026-11-23 14:34:22",
                            **kw,
                        ),
                        dict(
                            SkuNo="B",
                            Lot="M",
                            ActualQty=other,
                            ReceiptTime="2026-11-23 14:34:22",
                        ),
                    ],
                }
            ]
        }
    }


def test_complete_matches_each_sku_and_lot():
    s = summarize(rec(), cb())
    assert s["label"] == "已入库"
    assert s["quantities"][0]["actual"] == 101
    assert s["received_at"] == "2026-11-23T14:34:22+08:00"


def test_partial_and_rejected():
    assert summarize(rec(), cb(80))["label"] == "部分入库"
    assert summarize(rec(), cb(RejectionQty=1))["label"] == "部分入库"


def test_total_equal_but_wrong_sku_not_complete():
    assert summarize(rec(), cb(101, 0))["state"] == "exception"


def test_unknown_is_not_zero_and_no_callback_time_fallback():
    s = summarize(rec())
    assert s["state"] == "unknown" and s["quantities"] == []
    c = cb()
    c["raw_payload"]["PurchaseOrders"][0]["Items"][0]["ActualQty"] = "NaN"
    assert summarize(rec(), c)["state"] == "exception"
    c = cb()
    for i in c["raw_payload"]["PurchaseOrders"][0]["Items"]:
        i.pop("ReceiptTime")
    assert summarize(rec(), c)["received_at"] is None


def test_missing_line_and_wrong_order():
    c = cb()
    c["raw_payload"]["PurchaseOrders"][0]["Items"].pop()
    assert summarize(rec(), c)["state"] == "partial"
    c = cb()
    c["raw_payload"]["PurchaseOrders"][0]["ErpOrder"] = "another"
    assert summarize(rec(), c)["state"] == "unknown"


def test_outbound_unaffected_and_zero_receipt():
    r = rec()
    r["bill_type"] = "outbound"
    assert summarize(r, cb()) is None
    assert summarize(rec(), cb(0, 0))["label"] == "未入库"


def test_units_not_mixed():
    r = rec()
    r["items_json"][1]["unit"] = "盒"
    assert len(summarize(r, cb())["quantities"]) == 2
