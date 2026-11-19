"""Journal automatic FHTZ cancellation before dispatch; never replay a write."""
import logging
import uuid
from app.db.pool import get_pool

logger = logging.getLogger("union.sf_cancel_result")


async def cancel_and_record(bill_no, erp_order, shipment_id):
    from app.services.sf_outbound import cancel_outbound_order, query_outbound_status
    from app.services.sf_interception_worker import wms_state

    pool = await get_pool()
    op = uuid.uuid4()
    # If journaling fails, no cancellation is sent. The watcher retries next scan.
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO public.sf_auto_cancel_attempt(operation_id,bill_no,waybill_no,status)
               VALUES($1,$2,(SELECT waybill_no FROM ads_sf_push_record
                 WHERE bill_no=$2 AND bill_type='outbound' LIMIT 1),'PROCESSING')""", op, bill_no)
    response = {"head": "EXCEPTION"}
    state, sent, reason = "UNKNOWN", "UNKNOWN", "REMOTE_RESULT_UNKNOWN"
    confirmed = False
    try:
        response = await cancel_outbound_order(
            erp_order=erp_order, shipment_id=shipment_id, remark="金蝶反审核自动撤回")
        head = str(response.get("head") or "").upper()
        code = str(response.get("error_code") or "")
        sent = "NO" if code == "SF_REFUND_HOLD_OR_ORDER_BUSY" else "YES"
        if sent == "NO":
            state, reason = "FAILED", "LOCAL_HOLD_OR_BUSY"
        else:
            # A successful transport/accepted response alone is not cancellation evidence.
            try:
                current = wms_state(await query_outbound_status(erp_order), erp_order)
                if current == "1400":
                    state, reason, confirmed = "SUCCEEDED", "WMS_CANCEL_CONFIRMED", True
                elif head in ("ERROR", "ERR", "FAIL", "FAILED"):
                    state, reason = "FAILED", "WMS_REJECTED"
                else:
                    reason = "CANCEL_NOT_CONFIRMED"
            except Exception:
                logger.exception("Automatic cancellation confirmation unavailable: %s", bill_no)
    except Exception as exc:
        logger.exception("Automatic cancellation dispatch result unknown: %s", bill_no)
        response = {"head": "EXCEPTION", "error": str(exc)[:500]}
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """UPDATE public.sf_auto_cancel_attempt SET status=$2,request_sent=$3,
                   reason_code=$4,finished_at=now(),confirmed_at=CASE WHEN $5 THEN now() END,
                   response_head=$6,error_code=$7 WHERE operation_id=$1""",
                op, state, sent, reason, confirmed,
                str(response.get("head") or "")[:40], str(response.get("error_code") or "")[:100])
    except Exception:
        # Preserve the dispatch response for the watcher's existing transition.
        # A failed result write must not itself cause a second cancellation.
        # The persisted PROCESSING attempt becomes UNKNOWN on read after 10m.
        logger.exception("Automatic cancellation result persistence failed: %s %s", bill_no, op)
    return response

