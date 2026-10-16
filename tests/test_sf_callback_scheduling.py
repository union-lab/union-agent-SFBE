import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app import scheduler as sched
from app.services import sf_automation as service


def run(coro):
    """Pure mocked async operations: no network or Windows event loop required."""
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    finally:
        coro.close()
    raise AssertionError("Unexpected suspension")


def test_callback_runs_even_while_push_is_locked(monkeypatch):
    calls = []

    async def callback(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(service, "validate_outbound_callbacks", callback)
    monkeypatch.setattr(sched, "_sf_auto_push_lock", asyncio.Lock())
    monkeypatch.setattr(sched, "_sf_outbound_callback_lock", asyncio.Lock())

    async def scenario():
        async with sched._sf_auto_push_lock:
            await sched._run_sf_outbound_callbacks()
    run(scenario())
    assert calls == [{"build_budget_seconds": 45}]


def test_push_no_longer_calls_callbacks_or_logistics(monkeypatch):
    calls = []

    def operation(name):
        async def impl():
            calls.append(name)
            return 0
        return impl

    for name in ("ensure_table", "scan_and_push", "scan_and_push_outbound",
                 "validate_callbacks", "validate_outbound_callbacks", "_sync_recent_outstock_logistics"):
        monkeypatch.setattr(service, name, operation(name))
    monkeypatch.setattr(sched, "_sf_auto_push_lock", asyncio.Lock())
    run(sched._run_sf_auto_push())
    assert calls == ["ensure_table", "scan_and_push", "scan_and_push_outbound"]


def test_callback_error_releases_lock_and_next_run_works(monkeypatch):
    calls = []

    async def callback(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise RuntimeError("simulated error")

    monkeypatch.setattr(service, "validate_outbound_callbacks", callback)
    monkeypatch.setattr(sched, "_sf_outbound_callback_lock", asyncio.Lock())
    run(sched._run_sf_outbound_callbacks())
    assert not sched._sf_outbound_callback_lock.locked()
    run(sched._run_sf_outbound_callbacks())
    assert len(calls) == 2


def test_callback_reentrant_call_does_not_duplicate(monkeypatch):
    calls = []

    async def callback(**kwargs):
        calls.append(kwargs)
        await sched._run_sf_outbound_callbacks()

    monkeypatch.setattr(service, "validate_outbound_callbacks", callback)
    monkeypatch.setattr(sched, "_sf_outbound_callback_lock", asyncio.Lock())
    run(sched._run_sf_outbound_callbacks())
    assert len(calls) == 1


def test_scheduler_registers_independent_single_instance_jobs(monkeypatch):
    class Scheduler:
        running = False

        def __init__(self):
            self.jobs = []

        def add_job(self, func, trigger, **kwargs):
            self.jobs.append(SimpleNamespace(func=func, trigger=trigger, **kwargs))

        def start(self):
            self.running = True

        def get_jobs(self):
            return self.jobs

    scheduler = Scheduler()
    monkeypatch.setattr(sched, "scheduler", scheduler)
    monkeypatch.setattr(sched, "settings", SimpleNamespace(sf_module_enabled=True))
    monkeypatch.setenv("SCHEDULER_ENABLED", "true")
    sched.start_scheduler()
    jobs = {job.id: job for job in scheduler.jobs}
    assert len(jobs) == 8
    for name in ("sf_outbound_callbacks", "sf_inbound_callbacks", "sf_logistics_recheck"):
        assert jobs[name].max_instances == 1 and jobs[name].coalesce
    trigger = jobs["sf_outbound_callbacks"].trigger
    now = datetime(2026, 10, 16, 17, 9, 0, tzinfo=trigger.timezone)
    first = trigger.get_next_fire_time(None, now)
    second = trigger.get_next_fire_time(first, first + timedelta(seconds=1))
    assert first.second == 10 and second - first == timedelta(minutes=1)
    sched.start_scheduler()
    assert len(scheduler.jobs) == 8


def task(key, fallback=False):
    return (f"FHTZ{key}", key + 100, key, f"SF{key}", fallback, "CP")


def test_budget_finishes_inflight_order_defers_rest_then_rotates(monkeypatch):
    now = [100.0]
    calls = []

    async def create(bill, fid, rid, **kwargs):
        calls.append(rid)
        now[0] += 25
        return None

    monkeypatch.setattr(service, "time", SimpleNamespace(monotonic=lambda: now[0]))
    monkeypatch.setattr(service, "_outstock_last_attempt", {})
    monkeypatch.setattr(service, "_create_kingdee_outstock", create)
    tasks = [task(1), task(2), task(3), task(4)]
    result = run(service._run_outstock_tasks(tasks, budget_seconds=45))
    assert calls == [1, 2]  # 25 + 25: second write finishes instead of cancellation
    assert result == {"queued": 4, "attempted": 2, "deferred": 2}
    # Even tasks that produced no document must yield to never-attempted old ones.
    tasks.append(task(5))
    run(service._run_outstock_tasks(tasks, budget_seconds=45))
    assert calls == [1, 2, 3, 4]
    run(service._run_outstock_tasks([], budget_seconds=45))
    assert service._outstock_last_attempt == {}


def test_one_error_does_not_abort_other_orders_and_failure_no_success_alert(monkeypatch):
    calls = []
    alerts = []

    async def create(bill, fid, rid, **kwargs):
        calls.append(rid)
        if rid == 1:
            raise RuntimeError("login failed")
        return f"XSCK{rid}"

    async def alert(*args):
        alerts.append(args)

    monkeypatch.setattr(service, "_outstock_last_attempt", {})
    monkeypatch.setattr(service, "_create_kingdee_outstock", create)
    monkeypatch.setattr(service, "_send_wecom_alert", alert)
    run(service._run_outstock_tasks([task(1, True), task(2, True)], budget_seconds=45))
    assert calls == [1, 2]
    assert len(alerts) == 1 and "XSCK2" in alerts[0][1]


class Context:
    def __init__(self, owner, kind):
        self.owner, self.kind = owner, kind

    async def __aenter__(self):
        setattr(self.owner, self.kind, True)
        return self.owner

    async def __aexit__(self, *exc):
        setattr(self.owner, self.kind, False)


class LockConnection:
    def __init__(self, got=True, status="callback_ok"):
        self.got, self.status = got, status
        self.acquired = self.in_transaction = False
        self.keys = []

    def acquire(self):
        return Context(self, "acquired")

    def transaction(self):
        return Context(self, "in_transaction")

    async def fetchval(self, sql, *args):
        assert self.acquired and self.in_transaction
        if "pg_try_advisory_xact_lock" in sql:
            self.keys.append(args[0])
            return self.got
        assert "WHERE id = $1 AND bill_no = $2" in sql
        return self.status


@pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
def test_order_lock_held_on_same_connection_until_completion(monkeypatch, outcome):
    conn = LockConnection()

    async def pool():
        return conn

    async def create(*args, **kwargs):
        assert conn.acquired and conn.in_transaction
        if outcome == "error":
            raise RuntimeError("test")
        if outcome == "cancel":
            raise asyncio.CancelledError()
        return "XSCK1"

    monkeypatch.setattr(service, "get_pool", pool)
    monkeypatch.setattr(service, "_create_kingdee_outstock_locked", create)
    coro = service._create_kingdee_outstock("FHTZ1", 101, 1)
    if outcome == "success":
        assert run(coro) == "XSCK1"
    else:
        with pytest.raises(RuntimeError if outcome == "error" else asyncio.CancelledError):
            run(coro)
    assert not conn.acquired and not conn.in_transaction
    assert conn.keys == [int.from_bytes(hashlib.sha256(b"sf_outstock:FHTZ1").digest()[:8], "big", signed=True)]


@pytest.mark.parametrize("got,status", [(False, "callback_ok"), (True, "cancelled"),
                                        (True, "outstock_created"), (True, "outstock_failed")])
def test_busy_or_terminal_order_does_not_recreate(monkeypatch, got, status):
    conn = LockConnection(got, status)

    async def pool():
        return conn

    async def unexpected(*args, **kwargs):
        raise AssertionError("should not create")

    monkeypatch.setattr(service, "get_pool", pool)
    monkeypatch.setattr(service, "_create_kingdee_outstock_locked", unexpected)
    assert run(service._create_kingdee_outstock("FHTZ1", 101, 1)) is None
    assert not conn.acquired and not conn.in_transaction


def test_validation_keeps_mismatch_block_and_defers_without_losing_rows(monkeypatch):
    now = datetime.now(timezone.utc)
    rows = [{"id": i, "bill_no": f"FHTZ{i}", "status": "success", "items_json": '[{"qty": 2}]',
             "created_at": now - timedelta(days=1), "sf_receipt_id": "SO1", "fhtz_fid": i + 100,
             "outstock_no": None, "sf_wms_status_text": "已出库", "waybill_no": "", "carrier_code": "CP"}
            for i in range(1, 4)]
    writes = []
    calls = []
    clock = [100.0]

    class Conn:
        def acquire(self):
            return Context(self, "acquired")

        async def fetch(self, sql, *args):
            assert "ORDER BY created_at ASC, id ASC" in sql
            assert "'outstock_failed'" not in sql
            return [dict(r) for r in rows]

        async def fetchrow(self, sql, bill):
            if bill == "FHTZ3":
                return None  # WMS fallback retains original rules
            return {"raw_payload": json.dumps({"SaleOrders": [{
                "Items": [{"ActualQty": 1 if bill == "FHTZ1" else 2}], "Carrier": "CP",
            }]}), "received_at": now}

        async def execute(self, sql, *args):
            writes.append((sql, args))
            return "UPDATE 1"

    async def pool():
        return Conn()

    async def create(bill, fid, rid, **kwargs):
        calls.append((rid, kwargs["via_wms_fallback"]))
        clock[0] += 46
        return "XSCK2"

    async def alert(*args):
        pass

    async def unexpected(*args, **kwargs):
        raise AssertionError("historical logistics must not run in callback path")

    monkeypatch.setattr(service, "get_pool", pool)
    monkeypatch.setattr(service, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(service, "_outstock_last_attempt", {})
    monkeypatch.setattr(service, "_create_kingdee_outstock", create)
    monkeypatch.setattr(service, "_send_wecom_alert", alert)
    monkeypatch.setattr(service, "_sync_recent_outstock_logistics", unexpected)
    run(service.validate_outbound_callbacks())
    assert calls == [(2, False)]
    assert any("'callback_mismatch'" in sql and args == (1,) for sql, args in writes)
    assert not any("outstock_created" in sql for sql, _ in writes)
    # Deferred WMS fallback is rediscovered from DB, not held only in volatile queue.
    run(service.validate_outbound_callbacks())
    assert calls == [(2, False), (3, True)]


@pytest.mark.parametrize("case", ["already_audited", "duplicate", "gsp_rejected"])
def test_existing_document_never_repushed_or_gsp_bypassed(monkeypatch, case):
    writes = []
    audits = []

    class Conn:
        def acquire(self):
            return Context(self, "acquired")

        async def fetchval(self, *args):
            return "callback_ok"

        async def execute(self, sql, *args):
            writes.append((sql, args))

    class Kingdee:
        async def login(self):
            pass

        async def query(self, *args, **kwargs):
            if case == "duplicate":
                return [[1, "XSCK1", "C"], [2, "XSCK2", "B"]]
            return [[1, "XSCK1", "B" if case == "gsp_rejected" else "C"]]

        async def push(self, *args):
            raise AssertionError("existing document must not be pushed again")

        async def submit(self, *args):
            raise AssertionError("B/C status must not be submitted again")

        async def audit(self, form, data):
            audits.append((form, data))
            raise RuntimeError("GSP单据类型,存在还未复核完毕的,不允许!")

    async def noop(*args, **kwargs):
        pass

    async def pool():
        return Conn()

    monkeypatch.setattr(service, "get_pool", pool)
    monkeypatch.setattr(service, "KingdeeClient", lambda **kwargs: Kingdee())
    for name in ("_send_wecom_alert", "_save_outstock_waybill_and_delivery",
                 "_sync_push_record_logistics_from_outstock", "_try_sync_sale_order_waybill_from_outstock"):
        monkeypatch.setattr(service, name, noop)
    monkeypatch.setattr(asyncio, "sleep", noop)
    result = run(service._create_kingdee_outstock_locked("FHTZ1", 101, 1, "SF1", 123))
    if case == "already_audited":
        assert result == "XSCK1" and not audits
    elif case == "duplicate":
        assert result is None and not audits
        assert any("'duplicate_outstock'" in sql for sql, _ in writes)
    else:
        assert result is None and len(audits) == 1
        assert any("'outstock_failed'" in sql and "GSP" in args[0] for sql, args in writes)
        assert not any("SET status = 'outstock_created'" in sql for sql, _ in writes)


@pytest.mark.parametrize("enabled,module", [(False, True), (True, False)])
def test_disabled_service_does_not_register_new_jobs(monkeypatch, enabled, module):
    class Scheduler:
        running = False

        def add_job(self, *args, **kwargs):
            raise AssertionError("disabled module must not schedule jobs")

    monkeypatch.setattr(sched, "scheduler", Scheduler())
    monkeypatch.setattr(sched, "settings", SimpleNamespace(sf_module_enabled=module))
    monkeypatch.setenv("SCHEDULER_ENABLED", "true" if enabled else "false")
    sched.start_scheduler()
