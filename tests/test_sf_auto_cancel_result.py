from unittest.mock import AsyncMock
import pytest
from app.services import sf_auto_cancel_result as journal
from app.services import sf_outbound

class Context:
    def __init__(self, value): self.value = value
    async def __aenter__(self): return self.value
    async def __aexit__(self, *args): pass

@pytest.mark.asyncio
@pytest.mark.parametrize("response,state,expected,sent", [
    ({"head":"OK"},"1400","SUCCEEDED","YES"),
    ({"head":"OK"},"2400","UNKNOWN","YES"),
    ({"head":"ERROR"},"2400","FAILED","YES"),
    ({"head":"ERROR","error_code":"SF_REFUND_HOLD_OR_ORDER_BUSY"},"1400","FAILED","NO"),
])
async def test_confirmation_and_local_block(monkeypatch,response,state,expected,sent):
    conn=AsyncMock()
    class Pool:
        def acquire(self): return Context(conn)
    monkeypatch.setattr(journal,"get_pool",AsyncMock(return_value=Pool()))
    cancel=AsyncMock(return_value=response)
    monkeypatch.setattr(sf_outbound,"cancel_outbound_order",cancel)
    raw=f"<Response><SaleOrder><Header><ErpOrder>FHTZ1</ErpOrder><OrderStatus>{state}</OrderStatus></Header></SaleOrder></Response>"
    monkeypatch.setattr(sf_outbound,"query_outbound_status",AsyncMock(return_value={"head":"OK","raw":raw}))
    await journal.cancel_and_record("FHTZ1","FHTZ1","")
    assert conn.execute.await_args_list[0].args[0].startswith("INSERT")
    assert conn.execute.await_args.args[2:4] == (expected,sent)
    cancel.assert_awaited_once()

@pytest.mark.asyncio
async def test_journal_failure_prevents_dispatch(monkeypatch):
    monkeypatch.setattr(journal,"get_pool",AsyncMock(side_effect=RuntimeError("offline")))
    cancel=AsyncMock()
    monkeypatch.setattr(sf_outbound,"cancel_outbound_order",cancel)
    with pytest.raises(RuntimeError):
        await journal.cancel_and_record("FHTZ1","FHTZ1","")
    cancel.assert_not_awaited()

@pytest.mark.asyncio
async def test_timeout_recorded_unknown(monkeypatch):
    conn=AsyncMock()
    class Pool:
        def acquire(self):return Context(conn)
    monkeypatch.setattr(journal,"get_pool",AsyncMock(return_value=Pool()))
    monkeypatch.setattr(sf_outbound,"cancel_outbound_order",AsyncMock(side_effect=TimeoutError()))
    await journal.cancel_and_record("FHTZ1","FHTZ1","")
    assert conn.execute.await_args.args[2:4] == ("UNKNOWN","UNKNOWN")


@pytest.mark.asyncio
async def test_result_write_failure_preserves_response(monkeypatch):
    conn = AsyncMock()
    conn.execute.side_effect = [None, RuntimeError('result store unavailable')]
    class Pool:
        def acquire(self): return Context(conn)
    monkeypatch.setattr(journal, 'get_pool', AsyncMock(return_value=Pool()))
    response = {'head': 'OK'}
    cancel = AsyncMock(return_value=response)
    monkeypatch.setattr(sf_outbound, 'cancel_outbound_order', cancel)
    monkeypatch.setattr(sf_outbound, 'query_outbound_status', AsyncMock(side_effect=TimeoutError()))
    assert await journal.cancel_and_record('FHTZ1', 'FHTZ1', '') == response
    cancel.assert_awaited_once()
