from __future__ import annotations

from app.services import sf_automation


class _Kingdee:
    def __init__(self, sales_orders: dict[str, list]):
        self.sales_orders = sales_orders
        self.queries: list[tuple[str, str, str]] = []
        self.saves: list[tuple[str, dict]] = []

    async def query(self, form_id, field_keys, *, filter_string="", **_kwargs):
        self.queries.append((form_id, field_keys, filter_string))
        if form_id == "SAL_OUTSTOCK":
            return [
                ["XSDD20260829000223", "SF6048643216347"],
                ["XSDD20260829000223", "SF6048643216347"],
                ["XSDD20260829000224", "SF6048643216347"],
            ]
        if form_id == "SAL_SaleOrder":
            for bill_no, row in self.sales_orders.items():
                if bill_no in filter_string:
                    return [row]
            return []
        raise AssertionError(f"unexpected form: {form_id}")

    async def save(self, form_id, data):
        self.saves.append((form_id, data))
        return {"success": True}


def _run_immediate(coro):
    """Run a fully mocked coroutine without creating a Windows event loop."""
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    raise AssertionError("mocked coroutine unexpectedly suspended")


def test_sync_sale_order_waybill_updates_only_empty_source_orders() -> None:
    kingdee = _Kingdee({
        "XSDD20260829000223": [496406, "XSDD20260829000223", "C", ""],
        "XSDD20260829000224": [496407, "XSDD20260829000224", "C", " "],
    })

    result = _run_immediate(
        sf_automation.sync_sale_order_waybill_from_outstock(
            kingdee,
            "XSCK20260829000326",
            "SF6048643216347",
            log_prefix="test",
        )
    )

    assert result["source_orders"] == ["XSDD20260829000223", "XSDD20260829000224"]
    assert result["updated"] == ["XSDD20260829000223", "XSDD20260829000224"]
    assert not result["failed"]
    assert kingdee.saves == [
        ("SAL_SaleOrder", {
            "NeedUpDateFields": ["F_YLYL_Text9"],
            "IsDeleteEntry": "false",
            "Model": {"FID": 496406, "F_YLYL_Text9": "SF6048643216347"},
        }),
        ("SAL_SaleOrder", {
            "NeedUpDateFields": ["F_YLYL_Text9"],
            "IsDeleteEntry": "false",
            "Model": {"FID": 496407, "F_YLYL_Text9": "SF6048643216347"},
        }),
    ]
    assert all("FDocumentStatus" not in str(data) for _, data in kingdee.saves)


def test_sync_sale_order_waybill_never_overwrites_existing_values() -> None:
    kingdee = _Kingdee({
        "XSDD20260829000223": [496406, "XSDD20260829000223", "C", "SF6048643216347"],
        "XSDD20260829000224": [496407, "XSDD20260829000224", "C", "SF9999999999999"],
    })

    result = _run_immediate(
        sf_automation.sync_sale_order_waybill_from_outstock(
            kingdee,
            "XSCK20260829000326",
            "SF6048643216347",
            log_prefix="test",
        )
    )

    assert result["already_synced"] == ["XSDD20260829000223"]
    assert result["conflicts"] == [{
        "sale_order_no": "XSDD20260829000224",
        "current_waybill": "SF9999999999999",
        "target_waybill": "SF6048643216347",
    }]
    assert not result["failed"]
    assert kingdee.saves == []


def test_sync_sale_order_waybill_uses_outstock_waybill_when_record_is_empty() -> None:
    kingdee = _Kingdee({
        "XSDD20260829000223": [496406, "XSDD20260829000223", "C", ""],
        "XSDD20260829000224": [496407, "XSDD20260829000224", "C", ""],
    })

    result = _run_immediate(
        sf_automation.sync_sale_order_waybill_from_outstock(
            kingdee,
            "XSCK20260829000326",
            log_prefix="test",
        )
    )

    assert result["waybill_no"] == "SF6048643216347"
    assert result["updated"] == ["XSDD20260829000223", "XSDD20260829000224"]
