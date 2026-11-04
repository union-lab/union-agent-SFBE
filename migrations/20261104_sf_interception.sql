-- MANUAL ONLY. Additive tables; no changes to existing order data or schedulers.
-- Apply before deploying the SF guard, then market API, then publication.
BEGIN;
SET LOCAL lock_timeout = '3s';
CREATE TABLE IF NOT EXISTS sf_api_runtime (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    protocol integer NOT NULL,
    heartbeat_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS sf_api_hold (
    bill_no text PRIMARY KEY,
    hold_id uuid NOT NULL UNIQUE,
    owner text NOT NULL,
    request_id text NOT NULL,
    body_hash text NOT NULL,
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(owner, request_id)
);
CREATE TABLE IF NOT EXISTS sf_api_operation (
    operation_id uuid PRIMARY KEY,
    owner text NOT NULL,
    request_id text NOT NULL,
    body_hash text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('HOLD','CANCEL')),
    bill_no text NOT NULL,
    status text NOT NULL CHECK (status IN ('QUEUED','PROCESSING','SUCCEEDED','BLOCKED','REJECTED','UNKNOWN')),
    request_sent text NOT NULL DEFAULT 'NO' CHECK (request_sent IN ('NO','YES','UNKNOWN')),
    expected_version text,
    result jsonb NOT NULL DEFAULT '{}',
    receipt text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(owner, request_id)
);
-- A held fulfillment cannot be re-created or re-cancelled under another request ID.
-- Manual recovery/new fulfillment generations require a separate audited workflow.
CREATE UNIQUE INDEX IF NOT EXISTS sf_api_cancel_once
    ON sf_api_operation(bill_no) WHERE kind='CANCEL';
CREATE INDEX IF NOT EXISTS sf_api_operation_queue
    ON sf_api_operation(created_at) WHERE status='QUEUED';
CREATE TABLE IF NOT EXISTS sf_api_legacy_cancel (
    bill_no text PRIMARY KEY,
    attempted_at timestamptz NOT NULL DEFAULT now()
);
COMMIT;
