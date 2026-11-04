import json
from unittest.mock import AsyncMock

import pytest

from app.services import sf_interception_worker as worker
from app.services import sf_interception_guard as guard
from app.services import sf_outbound, sf_automation


def response(bill="FHTZ1", state="1100"):
    return {"head": "OK", "raw": f"""<Response><Head>OK</Head><Body>
    <SaleOrders><SaleOrder><Header><ErpOrder>{bill}</ErpOrder>
    <OrderStatus>{state}</OrderStatus></Header></SaleOrder></SaleOrders></Body></Response>"""}


@pytest.mark.parametrize("bad", [
    {"head": "ERROR"}, {"head": "OK", "raw": "<broken"},
    response("OTHER"), response(state=""),
    {"head": "OK", "raw": "<Response/>"},
])
def test_wms_evidence_never_assumes_absence(bad):
    with pytest.raises(Exception):
        worker.wms_state(bad, "FHTZ1")


def test_wms_requires_exact_order():
    assert worker.wms_state(response(state="1400"), "FHTZ1") == "1400"


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [
    None, {}, {"Result": {"ResponseStatus": {"IsSuccess": False}}},
    [{"error": "session lost"}], [["1", "XSCK1"]], [["1", "XSCK1", "C"]] * 501,
])
async def test_kd_errors_and_truncation_are_not_no_xsck(rows):
    kd = AsyncMock()
    kd._call.return_value = rows
    with pytest.raises(ValueError):
        await worker.strict_xscks(kd, "FHTZ1")


@pytest.mark.asyncio
async def test_xsck_exact_read_includes_all_document_states():
    kd = AsyncMock()
    kd._call.return_value = [["1", "XSCK1", "A"], ["1", "XSCK1", "A"], ["2", "XSCK2", "C"]]
    result = await worker.strict_xscks(kd, "FHTZ1")
    assert [r["bill_no"] for r in result] == ["XSCK1", "XSCK2"]
    query = json.loads(kd._call.await_args.args[1]["data"])
    assert query["FilterString"] == "FSrcBillNo = 'FHTZ1'"
    assert "FDocumentStatus =" not in query["FilterString"]


class Context:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *args):
        pass


@pytest.mark.asyncio
@pytest.mark.parametrize("busy,held", [(True, False), (False, True)])
async def test_dispatch_blocked_without_network(monkeypatch, busy, held):
    conn = AsyncMock()
    conn.transaction = lambda: Context(conn)
    conn.fetchval.side_effect = [False] if busy else [True, held]
    pool = AsyncMock()
    pool.acquire = lambda: Context(conn)
    monkeypatch.setattr(guard, "get_pool", AsyncMock(return_value=pool))
    network = AsyncMock()
    monkeypatch.setattr(sf_outbound, "_send_request_unchecked", network)
    xml = "<Request><SaleOrders><SaleOrder><ErpOrder>FHTZ1</ErpOrder></SaleOrder></SaleOrders></Request>"
    result = await sf_outbound._send_request("SALE_ORDER_SERVICE", xml)
    assert result["head"] == "ERROR"
    network.assert_not_awaited()


@pytest.mark.asyncio
async def test_inbound_transport_does_not_use_outbound_gate(monkeypatch):
    network = AsyncMock(return_value={"head": "OK"})
    gate_pool = AsyncMock(side_effect=AssertionError("inbound must remain independent"))
    monkeypatch.setattr(guard, "get_pool", gate_pool)
    monkeypatch.setattr(sf_outbound, "_send_request_unchecked", network)
    assert await sf_outbound._send_request("PURCHASE_ORDER_SERVICE", "<inbound/>") == {"head": "OK"}
    network.assert_awaited_once()


@pytest.mark.asyncio
async def test_automation_uses_same_gate(monkeypatch):
    conn = AsyncMock()
    conn.transaction = lambda: Context(conn)
    conn.fetchval.side_effect = [True, True]
    pool = AsyncMock()
    pool.acquire = lambda: Context(conn)
    monkeypatch.setattr(guard, "get_pool", AsyncMock(return_value=pool))
    network = AsyncMock()
    monkeypatch.setattr(sf_automation, "_sf_send_unchecked", network)
    xml = "<Request><SaleOrders><SaleOrder><ErpOrder>FHTZ1</ErpOrder></SaleOrder></SaleOrders></Request>"
    assert (await sf_automation._sf_send("SALE_ORDER_SERVICE", xml))[0] is False
    network.assert_not_awaited()
