"""Manual isolated PostgreSQL test, never run against a real business database.

Run with TEST_SF_DSN whose database name is exactly sf_interception_test and
MARKET_SF_SERVICE pointing at the reviewed market sf_interception.py source.
External clients are replaced by strict fakes; the container has no internet.
"""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse
from unittest.mock import AsyncMock

import asyncpg
from fastapi import HTTPException

from app.services import sf_interception_guard as guard
from app.services import sf_interception_worker as worker


class Body(SimpleNamespace):
    def model_dump(self, **kwargs):
        return vars(self)


def body(bill, request_id, **extra):
    return Body(bill_no=bill, request_id=request_id,
                scope="ENTIRE_WAREHOUSE_ORDER", reason="isolated test", **extra)


def wms(bill, state):
    return {"head": "OK", "raw": f"<Response><Head>OK</Head><Body><SaleOrders><SaleOrder>"
            f"<Header><ErpOrder>{bill}</ErpOrder><OrderStatus>{state}</OrderStatus></Header>"
            "</SaleOrder></SaleOrders></Body></Response>"}


async def main():
    dsn = os.environ["TEST_SF_DSN"]
    assert urlparse(dsn).path == "/sf_interception_test", "isolated test DB only"
    spec = importlib.util.spec_from_file_location("market_sf", os.environ["MARKET_SF_SERVICE"])
    market = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(market)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
    async def get_pool():
        return pool
    market.get_pool = guard.get_pool = worker.get_pool = get_pool
    try:
        async with pool.acquire() as conn:
            assert not await conn.fetchval("SELECT to_regclass('ads_sf_push_record')")
            await conn.execute(Path("migrations/20261104_sf_interception.sql").read_text())
            await conn.execute("""
                CREATE TABLE ads_sf_push_record(
                    id bigint GENERATED ALWAYS AS IDENTITY, bill_no text,
                    bill_type text DEFAULT 'outbound',erp_order text,sf_receipt_id text DEFAULT 'SO1',
                    status text DEFAULT 'success',outstock_no text,cancelled_at timestamptz,
                    sf_wms_status text DEFAULT '1100',updated_at timestamptz DEFAULT now());
                CREATE TABLE ods_kingdee_delivery_notice(
                    bill_no text,src_bill_no text,entry_id bigint,material_code text,
                    qty numeric,unit text,warehouse text,created_at timestamptz DEFAULT now());
                CREATE TABLE ods_sf_push_log(erp_order text,service_code text);
            """)
            for n in range(1, 18):
                await conn.execute("INSERT INTO ads_sf_push_record(bill_no,erp_order) VALUES($1,$1)", f"FHTZ{n}")
                await conn.execute(
                    """INSERT INTO ods_kingdee_delivery_notice
                       (bill_no,src_bill_no,entry_id,material_code,qty,unit,warehouse)
                       VALUES($1,'XSDD1',$2,'SKU',1.250,'盒','顺丰仓')""", f"FHTZ{n}", n,
                )
            await conn.execute(
                "INSERT INTO ods_kingdee_delivery_notice(bill_no,entry_id) VALUES('FHTZ99',99)"
            )
        await guard.heartbeat()
        h = await market.submit("app:a", "HOLD", body("FHTZ1", "hold-1"))
        assert h["status"] == "SUCCEEDED" and h["result"]["dispatch_at_acceptance"] == "IDLE"
        assert (await market.submit("app:a", "HOLD", body("FHTZ1", "hold-1")))["operation_id"] == h["operation_id"]
        try:
            await market.submit("app:a", "HOLD", body("FHTZ2", "hold-1"))
            raise AssertionError("body mismatch accepted")
        except HTTPException as exc:
            assert exc.status_code == 409
        xml = "<Request><SaleOrders><SaleOrder><ErpOrder>FHTZ1</ErpOrder></SaleOrder></SaleOrders></Request>"
        async with guard.outbound_gate(xml) as allowed:
            assert not allowed
        # Concurrent in-flight send + hold: hold is durable, truthful, and does not block another bill.
        xml2 = xml.replace("FHTZ1", "FHTZ2")
        async with guard.outbound_gate(xml2) as allowed:
            assert allowed
            busy = await market.submit("app:b", "HOLD", body("FHTZ2", "hold-2"))
            assert busy["result"]["dispatch_at_acceptance"] == "IN_FLIGHT"
            async with guard.outbound_gate(xml.replace("FHTZ1", "FHTZ3")) as other:
                assert other
        async with guard.outbound_gate(xml2) as allowed:
            assert not allowed
        # Hold may precede a WMS record.
        before = await market.submit("app:c", "HOLD", body("FHTZ99", "before-push"))
        assert not before["result"]["already_pushed"]
        async with guard.outbound_gate(xml.replace("FHTZ1", "FHTZ99")) as allowed:
            assert not allowed
        # Only owner app can see results, including delegated identities.
        assert (await market.operation("app:a:user:1", "hold-1"))["operation_id"] == h["operation_id"]
        try:
            await market.operation("app:b", "hold-1")
            raise AssertionError("cross-app leak")
        except HTTPException as exc:
            assert exc.status_code == 404
        links = await market.order_links("FHTZ1")
        assert links["complete"] is False and links["items"][0]["qty"] == "1.250"
        assert links["version"] == guard.version({
            "id": "1", "bill_no": "FHTZ1", "erp_order": "FHTZ1",
            "sf_receipt_id": "SO1", "status": "success", "sf_wms_status": "1100",
        })
        cb = body("FHTZ1", "cancel-1", hold_id=h["result"]["hold_id"], expected_version=links["version"])
        op = await market.submit("app:a", "CANCEL", cb)
        assert op["status"] == "QUEUED"
        assert (await market.submit("app:a", "CANCEL", cb))["operation_id"] == op["operation_id"]
        kd = AsyncMock()
        kd._call.return_value = []
        worker.KingdeeClient = lambda **kwargs: kd
        worker.sf_outbound.query_outbound_status = AsyncMock(side_effect=[wms("FHTZ1", "1100"), wms("FHTZ1", "1400")])
        worker.sf_outbound._send_request_unchecked = AsyncMock(return_value={"head": "OK", "raw": "<Response><Head>OK</Head></Response>"})
        async with pool.acquire() as conn:
            queued = await conn.fetchrow("SELECT * FROM sf_api_operation WHERE request_id='cancel-1'")
        await asyncio.gather(worker.execute_one(pool, queued), worker.execute_one(pool, queued))
        assert worker.sf_outbound._send_request_unchecked.await_count == 1
        completed = await market.operation("app:a", "cancel-1")
        assert completed["status"] == "SUCCEEDED" and completed["request_sent"] == "YES"
        assert "receipt" not in completed
        # No stock/outstock mutation; late facts downgrade the result for manual reconciliation.
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT status FROM ads_sf_push_record WHERE bill_no='FHTZ1'") == "success"
            await conn.execute("INSERT INTO ods_sf_push_log VALUES('FHTZ1','SALE_ORDER_OUTBOUND_DETAIL_PUSH_SERVICE')")
        assert (await market.operation("app:a", "cancel-1"))["status"] == "UNKNOWN"
        assert (await market.submit("app:a", "CANCEL", cb))["status"] == "UNKNOWN"
        # Worker error paths: strict KD errors, existing draft XSCK, packing,
        # old cancel attempt, callback, version race, send timeout, negative receipt.
        cases = ["kd_error", "xsck", "packed", "old_attempt", "callback", "changed", "timeout", "negative"]
        for index, case in enumerate(cases, 4):
            bill = f"FHTZ{index}"
            actor = f"app:case{index}"
            hold = await market.submit(actor, "HOLD", body(bill, f"h{index}"))
            links = await market.order_links(bill)
            cmd = await market.submit(actor, "CANCEL", body(
                bill, f"c{index}", hold_id=hold["result"]["hold_id"], expected_version=links["version"],
            ))
            kd._call.return_value = {"error": "down"} if case == "kd_error" else (
                [["1", "XSCK1", "A"]] if case == "xsck" else []
            )
            worker.sf_outbound.query_outbound_status = AsyncMock(return_value=wms(bill, "2400" if case == "packed" else "1100"))
            send = worker.sf_outbound._send_request_unchecked = AsyncMock(
                side_effect=TimeoutError("test") if case == "timeout" else None,
                return_value={"head": "ERROR", "raw": "<Response><Head>ERROR</Head></Response>"},
            )
            async with pool.acquire() as conn:
                if case == "old_attempt":
                    await conn.execute("INSERT INTO sf_api_legacy_cancel(bill_no) VALUES($1)", bill)
                if case == "callback":
                    await conn.execute("INSERT INTO ods_sf_push_log VALUES($1,'SALE_ORDER_OUTBOUND_DETAIL_PUSH_SERVICE')", bill)
                if case == "changed":
                    await conn.execute("UPDATE ads_sf_push_record SET sf_wms_status='1600' WHERE bill_no=$1", bill)
                row = await conn.fetchrow("SELECT * FROM sf_api_operation WHERE operation_id=$1", __import__("uuid").UUID(cmd["operation_id"]))
            await worker.execute_one(pool, row)
            result = await market.operation(actor, f"c{index}")
            assert result["status"] == ("UNKNOWN" if case in ("timeout", "negative") else "BLOCKED"), (case, result)
            assert send.await_count == (1 if case in ("timeout", "negative") else 0), case
            await worker.execute_one(pool, row)
            assert send.await_count == (1 if case in ("timeout", "negative") else 0), "replayed"
        # Expired executor: refuse new writes while original results remain queryable.
        # A legacy cancel must work even with a one-connection pool; its intent
        # is committed before IO and its session lock is released afterwards.
        single = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
        async def single_pool():
            return single
        guard.get_pool = single_pool
        try:
            async with asyncio.timeout(5):
                async with guard.outbound_gate(xml.replace("FHTZ1", "FHTZ16"), cancellation=True) as allowed:
                    assert allowed
            async with single.acquire() as c:
                assert await c.fetchval("SELECT EXISTS(SELECT 1 FROM sf_api_legacy_cancel WHERE bill_no='FHTZ16')")
                assert await c.fetchval("SELECT pg_try_advisory_lock($1)", guard.lock_key("FHTZ16"))
                await c.execute("SELECT pg_advisory_unlock($1)", guard.lock_key("FHTZ16"))
        finally:
            guard.get_pool = get_pool
            await single.close()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE sf_api_runtime SET heartbeat_at=now()-interval '1 hour'")
        try:
            await market.submit("app:late", "HOLD", body("FHTZ17", "late"))
            raise AssertionError("accepted without guard heartbeat")
        except HTTPException as exc:
            assert exc.status_code == 503
        assert (await market.operation("app:a", "hold-1"))["status"] == "SUCCEEDED"
        print(json.dumps({"status": "PASS", "real_postgres": True,
                          "external_writes": "mocked", "cases": 21}))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
