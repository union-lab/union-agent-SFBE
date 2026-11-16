-- Manual migration. No existing business tables are changed.
CREATE TABLE IF NOT EXISTS public.sf_order_node_event (
    bill_no text NOT NULL,
    node_code text NOT NULL,
    occurred_at timestamptz NOT NULL,
    source text NOT NULL,
    reference text NOT NULL DEFAULT '',
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (bill_no, node_code, occurred_at, source, reference)
);
CREATE INDEX IF NOT EXISTS sf_order_node_event_time_idx ON public.sf_order_node_event (occurred_at);
CREATE TABLE IF NOT EXISTS public.sf_order_node_poll (
    bill_no text PRIMARY KEY,
    checked_at timestamptz NOT NULL,
    complete boolean NOT NULL DEFAULT false,
    attempts integer NOT NULL DEFAULT 1
);
COMMENT ON TABLE public.sf_order_node_event IS 'Append-only observed events; 1700/2400/2900 are WMS event times, kingdee_waybill_saved is actual successful write, never inferred from existing values.';

