import asyncio

import httpx
import pytest

from app.clients import kingdee as module


@pytest.fixture
def clients(monkeypatch):
    module._SF_SESSIONS.clear()
    module._SF_LOGIN_LOCKS.clear()
    calls = []
    instances = []
    lost = [False]
    fail_login = [False]
    original = httpx.AsyncClient

    async def handle(request):
        name = request.url.path.split(".")[-3]
        calls.append(name)
        await asyncio.sleep(0)
        if name == "ValidateUser":
            if fail_login[0]:
                return httpx.Response(200, json={"LoginResultType": 0})
            return httpx.Response(200, json={"LoginResultType": 1},
                                  headers={"set-cookie": f"session={calls.count(name)}; Path=/"})
        if lost[0]:
            lost[0] = False
            return httpx.Response(200, json={"Result": {"ResponseStatus": {"MsgCode": 1}}})
        return httpx.Response(200, json=[])

    def http(**kwargs):
        instance = original(transport=httpx.MockTransport(handle), **kwargs)
        instances.append(instance)
        return instance

    monkeypatch.setattr(module.httpx, "AsyncClient", http)

    def make(**kwargs):
        return module.KingdeeClient(base_url=kwargs.pop("base_url", "https://example.test/K3Cloud"),
                                    acct_id=kwargs.pop("acct_id", "tenant"),
                                    username=kwargs.pop("username", "骆铭源"),
                                    password=kwargs.pop("password", "test-only"), **kwargs)

    yield make, calls, lost, fail_login
    module._SF_SESSIONS.clear()
    module._SF_LOGIN_LOCKS.clear()


@pytest.mark.asyncio
async def test_concurrent_task_clients_login_once_and_close_does_not_logout(clients):
    make, calls, _, _ = clients
    items = [make() for _ in range(20)]
    await asyncio.gather(*(c.login() for c in items))
    assert calls == ["ValidateUser"]
    assert all(c._cookies is items[0]._cookies for c in items)
    await items[0].aclose()
    await items[1].query("X", "FID")
    assert calls == ["ValidateUser", "ExecuteBillQuery"]
    for c in items[1:]:
        await c.aclose()


@pytest.mark.asyncio
async def test_expiry_refreshes_once(clients, monkeypatch):
    make, calls, _, _ = clients
    now = [1000]
    monkeypatch.setattr(module.time, "time", lambda: now[0])
    a, b = make(), make()
    await a.login()
    now[0] += module._SESSION_TTL + 1
    await asyncio.gather(a.login(), b.login())
    assert calls.count("ValidateUser") == 2
    await a.aclose()
    await b.aclose()


@pytest.mark.asyncio
async def test_session_loss_refresh_and_stale_failure_cannot_evict_new_session(clients):
    make, calls, lost, _ = clients
    a, b = make(), make()
    await a.login()
    await b.login()
    lost[0] = True
    await a.query("X", "FID")
    assert calls.count("ValidateUser") == 2
    b._invalidate_shared_session()
    await b.login()
    assert calls.count("ValidateUser") == 2 and a._cookies is b._cookies
    await a.aclose()
    await b.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("override", [dict(username="other"), dict(test=True),
    dict(acct_id="other"), dict(base_url="https://other.test"), dict(password="changed")])
async def test_credentials_and_environments_are_isolated(clients, override):
    make, calls, _, _ = clients
    a, b = make(), make(**override)
    await a.login()
    await b.login()
    assert calls.count("ValidateUser") == 2
    if override.get("username") or override.get("test"):
        await b.login()
        assert calls.count("ValidateUser") == 3
    await a.aclose()
    await b.aclose()


@pytest.mark.asyncio
async def test_failed_login_not_cached(clients):
    make, calls, _, fail = clients
    a = make()
    fail[0] = True
    with pytest.raises(module.KingdeeError):
        await a.login()
    assert not module._SF_SESSIONS
    fail[0] = False
    await a.login()
    assert calls.count("ValidateUser") == 2
    await a.aclose()


@pytest.mark.asyncio
async def test_switch_org_uses_private_session_and_logout_invalidates(clients):
    make, calls, _, _ = clients
    a, b = make(), make()
    await a.login()
    await b.login()
    await a.switch_org("org2")
    assert calls.count("ValidateUser") == 2
    assert a._cookies is not b._cookies
    await b.login()
    assert calls.count("ValidateUser") == 2
    await b.logout()
    assert not module._SF_SESSIONS
    await b.login()
    assert calls.count("ValidateUser") == 3
    await a.aclose()
    await b.aclose()
