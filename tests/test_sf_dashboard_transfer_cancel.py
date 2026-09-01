from __future__ import annotations

from app.api.routes import sf_dashboard
from app.services import sf_outbound


CANCELLED_XML = """<Response service="SALE_ORDER_STATUS_QUERY_SERVICE">
<Head>OK</Head><Body><SaleOrderStatusResponse><SaleOrders><SaleOrder>
<Header><OrderStatus>1400</OrderStatus><ErpOrder>FBDR008143</ErpOrder></Header>
</SaleOrder></SaleOrders></SaleOrderStatusResponse></Body></Response>"""

ACCEPTED_XML = CANCELLED_XML.replace("1400", "1100")


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


class _Conn:
    def __init__(self, record):
        self.record = record
        self.executed = []

    async def fetchrow(self, *_args):
        return self.record

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        return "UPDATE 1"


def _run_immediate(coro):
    """Run a fully mocked coroutine without creating a Windows event loop."""
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    raise AssertionError("mocked coroutine unexpectedly suspended")


def test_transfer_in_out_cancel_reconciles_already_cancelled(monkeypatch) -> None:
    conn = _Conn({
        "id": 42898,
        "bill_no": "FBDR008143",
        "erp_order": "FBDR008143",
        "status": "success",
        "bill_type": "transfer_in_out",
        "sf_receipt_id": "SO117036633423892",
    })

    async def fake_get_pool():
        return _Pool(conn)

    async def fake_query_status(erp_order):
        assert erp_order == "FBDR008143"
        return {"head": "OK", "raw": CANCELLED_XML}

    async def unexpected_cancel(*_args, **_kwargs):
        raise AssertionError("已取消订单不应再次调用取消接口")

    monkeypatch.setattr(sf_dashboard, "get_pool", fake_get_pool)
    monkeypatch.setattr(sf_outbound, "query_outbound_status", fake_query_status)
    monkeypatch.setattr(sf_outbound, "cancel_outbound_order", unexpected_cancel)

    result = _run_immediate(
        sf_dashboard.cancel_record(
            42898, sf_dashboard.CancelRequest(reason="中控平台手动取消")
        )
    )

    assert result["success"] is True
    assert result["sf_result"]["already_cancelled"] is True
    assert len(conn.executed) == 1
    sql, args = conn.executed[0]
    assert "sf_wms_status = '1400'" in sql
    assert "cancel_reason = $1" in sql
    assert args[0] == "中控平台手动取消"
    assert args[2] == 42898


def test_transfer_in_out_wms_query_reconciles_cancelled(monkeypatch) -> None:
    conn = _Conn({
        "id": 42898,
        "bill_no": "FBDR008143",
        "bill_type": "transfer_in_out",
    })

    async def fake_get_pool():
        return _Pool(conn)

    async def fake_query_status(erp_order):
        assert erp_order == "FBDR008143"
        return {"head": "OK", "raw": CANCELLED_XML}

    monkeypatch.setattr(sf_dashboard, "get_pool", fake_get_pool)
    monkeypatch.setattr(sf_outbound, "query_outbound_status", fake_query_status)

    result = _run_immediate(sf_dashboard.get_wms_status(42898))

    assert result["success"] is True
    assert result["order_status"] == "1400"
    assert result["order_status_text"] == "已取消"
    assert len(conn.executed) == 1
    sql, args = conn.executed[0]
    assert "THEN 'cancelled'" in sql
    assert args[0] == "1400"
    assert args[5] == 42898


def test_transfer_out_cancel_calls_outbound_cancel_service(monkeypatch) -> None:
    conn = _Conn({
        "id": 99,
        "bill_no": "ZJDB000099",
        "erp_order": "ZJDB000099",
        "status": "success",
        "bill_type": "transfer_out",
        "sf_receipt_id": "SO99",
    })
    cancel_calls = []

    async def fake_get_pool():
        return _Pool(conn)

    async def fake_query_status(erp_order):
        assert erp_order == "ZJDB000099"
        return {"head": "OK", "raw": ACCEPTED_XML}

    async def fake_cancel(erp_order, shipment_id):
        cancel_calls.append((erp_order, shipment_id))
        return {"head": "OK", "raw": "<Response><Head>OK</Head></Response>"}

    monkeypatch.setattr(sf_dashboard, "get_pool", fake_get_pool)
    monkeypatch.setattr(sf_outbound, "query_outbound_status", fake_query_status)
    monkeypatch.setattr(sf_outbound, "cancel_outbound_order", fake_cancel)

    result = _run_immediate(sf_dashboard.cancel_record(99))

    assert result["success"] is True
    assert cancel_calls == [("ZJDB000099", "SO99")]
    assert len(conn.executed) == 1


def test_outbound_transfer_types_have_labels_and_routing() -> None:
    assert sf_dashboard.OUTBOUND_BILL_TYPES == {
        "outbound",
        "transfer_out",
        "transfer_in_out",
    }
    assert sf_dashboard.BILL_TYPE_LABELS["transfer_out"] == "直接调拨单（出库）"
    assert sf_dashboard.BILL_TYPE_LABELS["transfer_in_out"] == "分布式调入单（出库）"
