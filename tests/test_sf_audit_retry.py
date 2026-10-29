from datetime import datetime, timedelta, timezone

import pytest

from app.services import sf_audit_retry as service
from tests.test_sf_callback_scheduling import run


def record(**changes):
    rec = dict(id=1, bill_no="FHTZ1", outstock_no="XSCK1", status="outstock_failed",
               bill_type="outbound", cancelled_at=None, unaudit_detected_at=None,
               updated_at=datetime.now(timezone.utc) - timedelta(hours=1),
               error_message="GSP单据类型,存在还未复核完毕的,不允许!")
    rec.update(changes)
    return rec


@pytest.mark.parametrize("changes", [dict(bill_type="manual_outbound"), dict(status="cancelled"),
    dict(outstock_no=None), dict(cancelled_at=True), dict(unaudit_detected_at=True),
    dict(error_message="余额不足"), dict(error_message="[sf-audit-retry:8] GSP存在还未复核完毕的"),
    dict(updated_at=datetime.now(timezone.utc))])
def test_only_due_gsp_failures_are_eligible(changes):
    assert not service.retry_due(record(**changes), datetime.now(timezone.utc))


def test_durable_exponential_backoff():
    now = datetime.now(timezone.utc)
    rec = record(error_message="[sf-audit-retry:3] GSP存在还未复核完毕的",
                 updated_at=now - timedelta(minutes=15))
    assert not service.retry_due(rec, now)
    assert service.retry_due(rec, now + timedelta(minutes=1))


@pytest.mark.parametrize("case", ["success", "already_c", "gsp", "timeout", "duplicate",
                                 "wrong_no", "source_unaudited", "lock", "changed", "not_c"])
def test_audit_only_recovery(monkeypatch, case):
    events = []
    rec = record()

    class Context:
        async def __aenter__(self):
            return conn

        async def __aexit__(self, *args):
            pass

    class Conn:
        def acquire(self):
            return Context()

        async def fetchrow(self, *args):
            events.append("read")
            if case == "changed" and events.count("read") == 2:
                return record(status="cancelled")
            return rec

        async def fetchval(self, sql, *args):
            if "advisory" in sql:
                events.append("lock")
                return case != "lock"
            events.append(("claim", args))
            return 1

        async def execute(self, sql, *args):
            events.append((sql, args))

    class KD:
        async def login(self):
            events.append("login")

        async def aclose(self):
            events.append("close")

        async def query(self, form, *args, **kwargs):
            if form == "SAL_DELIVERYNOTICE":
                return [[101, "B" if case == "source_unaudited" else "C"]]
            if case == "duplicate":
                return [[1, "XSCK1", "B"], [2, "XSCK2", "B"]]
            if case == "wrong_no":
                return [[1, "XSCK2", "B"]]
            status = "C" if case == "already_c" or ("audit" in events and case != "not_c") else "B"
            return [[1, "XSCK1", status]] * 2

        async def audit(self, form, data):
            assert form == "SAL_OUTSTOCK" and data == {"Ids": "1"}
            assert any(isinstance(e, tuple) and e[0] == "claim" for e in events)
            events.append("audit")
            if case in ("gsp", "timeout"):
                raise RuntimeError("GSP暂未就绪" if case == "gsp" else "timeout")

    conn = Conn()
    monkeypatch.setattr(service, "KingdeeClient", lambda **kwargs: KD())
    result = run(service._retry_one(conn, 1))
    assert result == (case in ("success", "already_c"))
    writes = [e for e in events if isinstance(e, tuple)]
    assert any("status='auto_resolved'" in sql for sql, _ in writes) == result
    assert ("audit" in events) == (case in ("success", "gsp", "timeout", "not_c"))
    if case != "lock":
        assert any("pg_advisory_unlock" in sql for sql, _ in writes)
    if case in ("gsp", "timeout", "not_c"):
        assert any("补审结果:" in str(args) for _, args in writes)


def test_budget_and_rate_limit(monkeypatch):
    now = [0]
    calls = []

    class Conn:
        def acquire(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def fetch(self, sql):
            assert "bill_type='outbound'" in sql
            return [record(id=n) for n in range(10)]

    async def pool():
        return Conn()

    async def retry(pool, rid):
        calls.append(rid)
        now[0] += 20
        return True

    monkeypatch.setattr(service, "get_pool", pool)
    monkeypatch.setattr(service, "_retry_one", retry)
    monkeypatch.setattr(service.time, "monotonic", lambda: now[0])
    assert run(service.retry_failed_outstock_audits()) == {"attempted": 2, "resolved": 2}
    assert calls == [0, 1]
    calls.clear()
    run(service.retry_failed_outstock_audits(budget_seconds=1000))
    assert len(calls) == 3
