CREATE TABLE IF NOT EXISTS public.sf_auto_cancel_attempt (
 operation_id uuid PRIMARY KEY,
 bill_no text NOT NULL,
 waybill_no text,
 status text NOT NULL CHECK(status IN ('PROCESSING','SUCCEEDED','FAILED','UNKNOWN')),
 request_sent text NOT NULL DEFAULT 'UNKNOWN',
 reason_code text,
 attempted_at timestamptz NOT NULL DEFAULT now(),
 finished_at timestamptz,
 confirmed_at timestamptz,
 response_head text,
 error_code text
);
CREATE INDEX IF NOT EXISTS sf_auto_cancel_bill_idx ON public.sf_auto_cancel_attempt(bill_no,attempted_at DESC);
CREATE INDEX IF NOT EXISTS sf_auto_cancel_waybill_idx ON public.sf_auto_cancel_attempt(waybill_no,attempted_at DESC);

