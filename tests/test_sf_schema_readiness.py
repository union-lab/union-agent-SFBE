from types import SimpleNamespace

import pytest

from app import main
from app.services import sf_automation as service


def run(coro):
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    finally:
        coro.close()
    raise AssertionError("mocked coroutine unexpectedly suspended")


@pytest.mark.parametrize("failure", [None, RuntimeError("missing column"), TimeoutError()])
def test_schema_check_is_read_only_bounded_and_never_repairs(monkeypatch, failure):
    calls = []

    class Connection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            calls.append("released")

        async def execute(self, sql, *, timeout):
            assert timeout == 10
            assert sql.lstrip().startswith("SELECT ")
            assert sql.rstrip().endswith("LIMIT 0")
            assert all(word not in sql.upper() for word in ("ALTER ", "CREATE ", "UPDATE ", "DELETE "))
            for column in ("sf_wms_status", "fhtz_fid", "outstock_no", "unaudit_detected_at"):
                assert column in sql
            calls.append("read")
            if failure is not None:
                raise failure

    def acquire(*, timeout):
        assert timeout == 10
        calls.append("acquire")
        return Connection()

    async def pool():
        return SimpleNamespace(acquire=acquire)

    async def forbidden_ddl():
        pytest.fail("schema readiness must never execute DDL")

    monkeypatch.setattr(service, "get_pool", pool)
    monkeypatch.setattr(service, "ensure_table", forbidden_ddl)
    if failure is None:
        run(service.check_push_record_schema())
    else:
        with pytest.raises(type(failure)):
            run(service.check_push_record_schema())
    assert calls == ["acquire", "read", "released"]


@pytest.mark.parametrize("failure", [None, RuntimeError("schema is not ready"), TimeoutError()])
def test_startup_checks_schema_before_jobs_and_fails_closed(monkeypatch, failure):
    calls = []

    def async_step(name):
        async def step():
            calls.append(name)
            if name == "schema" and failure is not None:
                raise failure
        return step

    monkeypatch.setattr(main, "init_pool", async_step("init_pool"))
    monkeypatch.setattr(main, "close_pool", async_step("close_pool"))
    monkeypatch.setattr(main, "check_push_record_schema", async_step("schema"))
    monkeypatch.setattr(main, "start_scheduler", lambda: calls.append("start"))
    monkeypatch.setattr(main, "stop_scheduler", lambda: calls.append("stop"))
    monkeypatch.setattr(main, "kingdee_client", SimpleNamespace(aclose=async_step("close_kd")))
    monkeypatch.setattr(main, "kingdee_test_client", SimpleNamespace(aclose=async_step("close_test_kd")))

    async def scenario():
        async with main.lifespan(main.app):
            calls.append("serving")

    if failure is None:
        run(scenario())
        assert calls == ["init_pool", "schema", "start", "serving", "stop",
                         "close_kd", "close_test_kd", "close_pool"]
    else:
        with pytest.raises(type(failure)):
            run(scenario())
        assert calls == ["init_pool", "schema", "close_pool"]
