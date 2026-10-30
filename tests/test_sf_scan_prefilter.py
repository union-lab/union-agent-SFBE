import pytest

from app.services import sf_automation as service
from tests.test_sf_callback_scheduling import run


@pytest.mark.parametrize("outbound", [True, False])
@pytest.mark.parametrize("has_new", [False, True])
def test_scan_skips_525_handled_orders_before_details(monkeypatch, outbound, has_new):
    calls = []
    failed = []
    handled = [f"OLD{i}" for i in range(525)]
    candidates = handled + ["BLACK"] + (["NEW"] if has_new else [])
    bill_type = service.OUTBOUND_BILL_TYPE if outbound else "transfer"

    class Pool:
        def acquire(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def fetch(self, sql, *args):
            assert args[0] == bill_type
            assert "'failed', 'cancelled_by_unaudit', 'unaudit_cancel_failed'" in sql
            if len(args) == 2:
                assert args[1] == candidates
            return [{"bill_no": bno} for bno in handled]

    class Kingdee:
        async def login(self):
            pass

        async def query(self, form, fields, **kwargs):
            calls.append((fields, kwargs))
            if "FBillNo='" not in kwargs["filter_string"]:
                return [[1, bno] if outbound else [bno] for bno in candidates]
            assert kwargs["filter_string"] == "FBillNo='NEW'"
            if fields == "FBillNo,FMaterialId.FNumber":
                return [["NEW", "SKU1"], ["NEW", "SKU2"]]
            return [[1] * 28]  # truncated detail must still be rejected

    async def pool():
        return Pool()

    async def mark(*args):
        failed.append(args[1])

    monkeypatch.setattr(service, "get_pool", pool)
    monkeypatch.setattr(service, "KingdeeClient", lambda **kwargs: Kingdee())
    monkeypatch.setattr(service, "_push_blacklist", lambda: {"BLACK"})
    monkeypatch.setattr(service, "_mark_failed", mark)
    if not outbound:
        cfg = dict(service.BILL_CONFIGS["STK_TransferDirect_inbound"])
        monkeypatch.setattr(service, "BILL_CONFIGS", {"test": cfg})
    fn = service.scan_and_push_outbound if outbound else service.scan_and_push
    assert run(fn()) == 0
    assert len(calls) == (3 if has_new else 1)
    assert failed == (["NEW"] if has_new else [])


def test_filter_preserves_order_and_bill_type_boundary():
    class Pool:
        def acquire(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def fetch(self, sql, bill_type, candidates):
            assert bill_type == "transfer_out"
            assert "bill_type = $1" in sql and "ANY($2::text[])" in sql
            return [{"bill_no": "DONE"}]

    assert run(service._filter_scan_candidates(
        Pool(), "transfer_out", ["NEW", "DONE", "RETRY", "BLACK"], {"BLACK"},
    )) == ["NEW", "RETRY"]
