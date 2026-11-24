from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import sf_dashboard


def run(coro):
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    raise AssertionError("mock coroutine suspended")


@pytest.mark.parametrize(
    "state,expected", [("complete", [1, 4]), ("partial", [2]), ("unknown", [3])]
)
def test_filter_before_pagination_and_reuses_display(monkeypatch, state, expected):
    records = [
        dict(id=i, bill_no=f"P{i}", erp_order=f"P{i}", bill_type="purchase_in",
             items_json=[dict(sku="A", lot="L", qty=10, unit="盒")])
        for i in range(1, 7)
    ]
    callbacks = [
        dict(id=i, erp_order=f"P{i}", received_at="2026-11-24", raw_payload={
            "PurchaseOrders": [{"ErpOrder": f"P{i}", "Items": [
                dict(SkuNo="A", Lot="L", ActualQty=q)
            ]}]
        })
        for i, q in [(1, 10), (2, 5), (4, 10), (5, 0), (6, "NaN")]
    ]
    # 第一页只放一单，但匹配ID/总数必须包含后续页。
    selected = [r for r in records if r["id"] in expected][1:2]
    conn = AsyncMock()
    conn.fetch.side_effect = [records, callbacks, selected]
    conn.fetchval.return_value = len(expected)

    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield conn

    monkeypatch.setattr(sf_dashboard, "get_pool", AsyncMock(return_value=Pool()))
    result = run(sf_dashboard.list_records(
        page=2, page_size=1, inbound_state=state, bill_type="purchase_in",
        search="P", date_from="2026-11-01", date_to="2026-11-24",
    ))
    candidate_sql, *candidate_params = conn.fetch.call_args_list[0].args
    assert "LIMIT" not in candidate_sql
    assert "r.bill_type = ANY" in candidate_sql
    assert candidate_params[:4] == ["purchase_in", "%P%", "2026-11-01", "2026-11-24"]
    assert "manual_inbound" in candidate_params[-1]
    assert conn.fetchval.call_args.args[-1] == expected
    page_sql, *page_params = conn.fetch.call_args_list[2].args
    assert "r.id = ANY" in page_sql
    assert "ORDER BY r.created_at DESC, r.id DESC" in page_sql
    assert page_params[-3:] == [expected, 1, 1]
    assert result["total"] == len(expected)
    assert [r["id"] for r in result["items"]] == [r["id"] for r in selected]
    assert all(r["inbound_receipt"]["state"] == state for r in result["items"])


def test_invalid_state_rejected_before_database():
    app = FastAPI()
    app.include_router(sf_dashboard.router)
    with TestClient(app) as client:
        assert client.get("/api/sf/dashboard/records?inbound_state=bad").status_code == 422


def test_no_filter_keeps_existing_paginated_query(monkeypatch):
    conn = AsyncMock()
    conn.fetch.return_value = [dict(id=7, bill_no="O7", bill_type="outbound")]
    conn.fetchval.return_value = 1

    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield conn

    monkeypatch.setattr(sf_dashboard, "get_pool", AsyncMock(return_value=Pool()))
    result = run(sf_dashboard.list_records(page=1, page_size=20))
    assert conn.fetch.call_count == 1
    assert "LIMIT" in conn.fetch.call_args.args[0]
    assert result["items"][0]["inbound_receipt"] is None
