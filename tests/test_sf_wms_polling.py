from datetime import datetime, timedelta, timezone

import pytest

from app.api.routes import sf_dashboard
from app.services import sf_outbound
from app.services.sf_wms_polling import WmsPollQueue


NOW = datetime(2026, 10, 12, 2, tzinfo=timezone.utc)


def run_immediate(coro):
    """所有 I/O 均已模拟，沿用仓库现有测试方式避免 Windows 事件循环阻塞。"""
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    finally:
        coro.close()
    raise AssertionError("mocked coroutine unexpectedly suspended")


def record(key, *, created=None, text="已检验打包", updated=None):
    return {
        "id": key, "bill_no": f"FHTZ{key}", "status": "success",
        "sf_wms_status_text": text,
        "created_at": created or NOW - timedelta(days=1, seconds=1000 - key),
        "updated_at": updated or NOW,
    }


def test_old_orders_survive_new_orders_and_recent_business_updates():
    queue = WmsPollQueue()
    records = [record(i) for i in range(1, 121)]
    seen = []
    for cycle in range(3):
        now = NOW + timedelta(minutes=cycle * 5)
        # 每轮不断加入空状态新单；旧单 updated_at 刚被其他业务更新。
        records.extend(record(1000 + cycle * 40 + i, created=now, text="") for i in range(40))
        for row in records:
            row["updated_at"] = now
        batch, _ = queue.select(records, limit=40, now=now)
        seen.extend(row["id"] for row in batch)
        for row in batch:
            queue.mark_attempt(row["id"], now)
    assert seen == list(range(1, 121))


def test_failed_attempts_also_yield_to_unchecked_orders():
    queue = WmsPollQueue()
    records = [record(i, text="") for i in range(80)]
    first, _ = queue.select(records, limit=40, now=NOW)
    for row in first:
        queue.mark_attempt(row["id"], NOW)
    # 即使第一批都失败且十分钟冷却到期，也不能继续霸占队头。
    second, _ = queue.select(records, limit=40, now=NOW + timedelta(minutes=10))
    assert [r["id"] for r in second] == list(range(40, 80))


def test_cooldown_and_terminal_recheck():
    queue = WmsPollQueue()
    row = record(1)
    queue.mark_attempt(1, NOW)
    assert queue.select([row], limit=40, now=NOW + timedelta(minutes=9)) == ([], 0)
    assert queue.select([row], limit=40, now=NOW + timedelta(minutes=10)) == ([row], 1)
    terminal = record(2, text="已出库", updated=NOW - timedelta(hours=1))
    assert queue.select([terminal], limit=40, now=NOW) == ([], 0)
    assert queue.select([terminal], limit=40, now=NOW + timedelta(hours=1)) == ([terminal], 1)


def test_queue_prunes_finished_records_and_restart_starts_with_oldest():
    queue = WmsPollQueue()
    queue.mark_attempt(99, NOW)
    rows = [record(2), record(1)]
    assert queue.select(rows, limit=1, now=NOW)[0][0]["id"] == 1
    assert 99 not in queue.last_attempt
    assert WmsPollQueue().select(rows, limit=1, now=NOW)[0][0]["id"] == 1


class Connection:
    def __init__(self, rows):
        self.rows = rows
        self.writes = []
        self.sql = ""

    async def fetch(self, sql, *args):
        self.sql = sql
        assert args == (sorted(sf_dashboard.OUTBOUND_BILL_TYPES),)
        return self.rows

    async def execute(self, sql, *args):
        self.writes.append((sql, args))
        return "UPDATE 1"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return self.conn


@pytest.fixture
def batch_env(monkeypatch):
    import asyncio

    conn = Connection([record(i) for i in range(1, 4)])
    sleeps = []

    async def get_pool():
        return Pool(conn)

    async def sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(sf_dashboard, "get_pool", get_pool)
    monkeypatch.setattr(sf_dashboard, "_wms_poll_queue", WmsPollQueue())
    monkeypatch.setattr(sf_dashboard, "_wms_poll_lock", asyncio.Lock())
    monkeypatch.setattr(asyncio, "sleep", sleep)
    return conn, sleeps


@pytest.mark.parametrize("failure", ["error", "empty", "exception"])
def test_batch_failures_rotate_and_stay_rate_limited(batch_env, monkeypatch, failure):
    conn, sleeps = batch_env
    calls = []

    async def query(bill):
        calls.append(bill)
        if failure == "exception":
            raise TimeoutError("test timeout")
        if failure == "error":
            return {"head": "ERR"}
        return {"head": "OK", "raw": "<Response/>"}

    monkeypatch.setattr(sf_outbound, "query_outbound_status", query)
    first = run_immediate(sf_dashboard.sync_wms_status_batch(limit=2))
    second = run_immediate(sf_dashboard.sync_wms_status_batch(limit=2))
    assert first["total"] == 2 and second["total"] == 1
    assert calls == ["FHTZ1", "FHTZ2", "FHTZ3"]
    assert sleeps == [1.1] * 3
    assert conn.writes == []
    assert "updated_at <" not in conn.sql
    assert "LIMIT" not in conn.sql
    assert "created_at DESC" not in conn.sql


def test_success_records_wms_without_changing_creation_rules(batch_env, monkeypatch):
    conn, sleeps = batch_env

    async def query(_bill):
        return {"head": "OK", "raw": ""}

    monkeypatch.setattr(sf_outbound, "query_outbound_status", query)
    monkeypatch.setattr(sf_dashboard, "_parse_wms_header", lambda _: {
        "OrderStatus": "2900", "Carrier": "ZT", "WmsCompleteTime": "2026-10-11 10:10:35",
    })
    result = run_immediate(sf_dashboard.sync_wms_status_batch(limit=1))
    assert result == {"total": 1, "success": 1, "failed": 0, "skipped": 0}
    assert sleeps == [1.1]
    assert len(conn.writes) == 1
    sql, args = conn.writes[0]
    assert "WHERE id = $6" in sql
    assert args[:2] == ("2900", "已出库")
    assert args[-1] == 1
    assert "THEN 'cancelled'" in sql
    assert "outstock_created_no_callback" in sql


def test_manual_and_scheduled_batches_cannot_overlap(batch_env):
    conn, _ = batch_env
    async def overlap():
        async with sf_dashboard._wms_poll_lock:
            return await sf_dashboard.sync_wms_status_batch()

    result = run_immediate(overlap())
    assert result["busy"] is True
    assert conn.sql == ""


@pytest.mark.parametrize("limit", [0, -1, 201])
def test_invalid_limit_does_not_query(batch_env, limit):
    conn, _ = batch_env
    with pytest.raises(ValueError):
        run_immediate(sf_dashboard.sync_wms_status_batch(limit=limit))
    assert conn.sql == ""
