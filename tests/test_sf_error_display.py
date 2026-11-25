from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from app.api.routes import sf_dashboard
from app.services import sf_outbound
from app.services.sf_error_display import fill_push_error, response_error


def xml(note="系统不能识别[中袋]的商品单位", result="2", bill="P1"):
    return (
        '<Response><Head>PART</Head><Body><SaleOrderResponse><SaleOrders>'
        f'<SaleOrder><Result>{result}</Result><Note>{note}</Note>'
        f'<ErpOrder>{bill}</ErpOrder></SaleOrder>'
        '</SaleOrders></SaleOrderResponse></Body></Response>'
    )


def run(coro):
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    raise AssertionError("mock coroutine suspended")


def mock_pool(monkeypatch, conn):
    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield conn
    monkeypatch.setattr(sf_dashboard, "get_pool", AsyncMock(return_value=Pool()))


def test_order_error_and_namespace():
    assert response_error(xml(), ["P1"]) == "系统不能识别[中袋]的商品单位"
    assert response_error(xml().replace('<Response>', '<Response xmlns="urn:sf">'), ["P1"])
    assert response_error(xml("订单不存在"), ["P1"]) == "订单不存在"


def test_does_not_show_other_orders_or_success_notes():
    assert response_error(xml(), ["P2"]) is None
    assert response_error(xml("处理成功", result="1"), ["P1"]) is None
    assert response_error(xml().replace('SaleOrder', 'PurchaseOrder'), ["P1"])


@pytest.mark.parametrize("raw", [None, "", "<invalid", "<html>502 Bad Gateway</html>"])
def test_missing_malformed_response_has_safe_fallback(raw):
    assert response_error(raw) is None


def test_top_level_error():
    raw = '<Response><Head>ERR</Head><Error code="E01">请求格式错误</Error></Response>'
    assert response_error(raw) == "请求格式错误（错误码：E01）"


def test_existing_business_error_and_non_failed_preserved():
    rec = dict(status="failed", bill_no="P1", sf_response=xml(), error_message="金蝶审核失败")
    fill_push_error(rec)
    assert rec['error_message'] == "金蝶审核失败"
    rec.update(status="success", error_message=None)
    fill_push_error(rec)
    assert rec['error_message'] is None


def test_history_list_and_detail_fill_without_database_writes(monkeypatch):
    record = dict(id=1, bill_no="P1", erp_order="P1", bill_type="outbound",
                  status="failed", error_message=None, sf_response=xml(), outstock_no=None)
    conn = AsyncMock()
    conn.fetch.return_value = [record]
    conn.fetchval.return_value = 1
    mock_pool(monkeypatch, conn)
    result = run(sf_dashboard.list_records(page=1, page_size=20))
    assert result['items'][0]['error_message'] == "系统不能识别[中袋]的商品单位"
    assert 'sf_response' not in result['items'][0]
    conn.fetchrow.return_value = record
    conn.fetch.return_value = []
    detail = run(sf_dashboard.get_record_detail(1))
    assert detail['error_message'] == result['items'][0]['error_message']
    assert detail['sf_response'] == xml()
    conn.execute.assert_not_called()


@pytest.mark.parametrize("raw,expected", [
    (xml("订单不存在"), "订单不存在"),
    ("<html>502</html>", "顺丰返回异常，未提供具体错误原因"),
])
def test_wms_query_returns_specific_reason(monkeypatch, raw, expected):
    conn = AsyncMock()
    conn.fetchrow.return_value = dict(id=1, bill_no="P1", bill_type="outbound")
    mock_pool(monkeypatch, conn)
    monkeypatch.setattr(sf_outbound, "query_outbound_status", AsyncMock(return_value={
        "head": "PART", "raw": raw,
    }))
    result = run(sf_dashboard.get_wms_status(1))
    assert result['success'] is False
    assert result['message'] == expected
    conn.execute.assert_not_called()
