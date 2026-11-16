from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from app.services import sf_node_history as h
from app.services import sf_outbound


def xml(steps, bill="F1", result="1"):
    return (f"<Response><SaleOrders><SaleOrder><Result>{result}</Result>"
            f"<ErpOrder>{bill}</ErpOrder><Header><WmsPushTime>2026-11-16 10:00:00</WmsPushTime></Header>"
            f"<Steps>{steps}</Steps></SaleOrder></SaleOrders></Response>")


def step(code, when):
    return f"<Step><Status>{code}</Status><EventTime>{when}</EventTime></Step>"


def test_events_preserve_repeated_milestones_dedupe_and_timezone():
    raw = xml(step("2400", "2026-11-16 10:00:00") * 2
              + step("2400", "2026-11-16 10:03:00")
              + step("2900", "invalid") + step("2300", "2026-11-16 10:00:00"))
    events, returned = h.extract_events(raw, {"F1"})
    assert returned == {"F1"}
    assert len(events) == 2
    assert events[0][2].astimezone(timezone.utc) == datetime(2026, 11, 16, 2, tzinfo=timezone.utc)


def test_missing_steps_never_inferred_from_header_and_wrong_order_rejected():
    assert h.extract_events(xml(""), {"F1"}) == ([], {"F1"})
    assert h.extract_events(xml(step("1700", "2026-11-16 10:00:00"), bill="other"), {"F1"}) == ([], set())
    assert h.extract_events(xml(step("1700", "2026-11-16 10:00:00"), result="0"), {"F1"}) == ([], set())


@pytest.mark.asyncio
async def test_storage_failure_does_not_break_live_status(monkeypatch):
    response = {"head": "OK", "raw": xml("")}
    monkeypatch.setattr(sf_outbound, "_send_request", AsyncMock(return_value=response))
    monkeypatch.setattr(h, "store_response", AsyncMock(side_effect=RuntimeError("database unavailable")))
    assert await sf_outbound.query_outbound_status("F1") == response


@pytest.mark.asyncio
async def test_failed_sf_response_is_not_archived(monkeypatch):
    response = {"head": "ERR", "raw": "invalid"}
    monkeypatch.setattr(sf_outbound, "_send_request", AsyncMock(return_value=response))
    persist = AsyncMock()
    monkeypatch.setattr(h, "safe_store_response", persist)
    assert await sf_outbound.query_outbound_status("F1") == response
    persist.assert_not_awaited()


@pytest.mark.asyncio
async def test_waybill_event_failure_is_isolated(monkeypatch):
    monkeypatch.setattr(h, "get_pool", AsyncMock(side_effect=RuntimeError("offline")))
    await h.record_waybill_saved("XS1", "SO1", "SF1", datetime.now(timezone.utc))

