CREATE TABLE IF NOT EXISTS demand_jobs (
    id TEXT PRIMARY KEY,
    image TEXT NOT NULL,
    command JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count INTEGER NOT NULL DEFAULT 0,
    manifest_root_cid TEXT,
    manifest_providers JSONB,
    manifest_enc_meta TEXT,
    lease_agent TEXT,
    lease_expires_at TIMESTAMPTZ,
    lease_token BIGINT NOT NULL DEFAULT 0,
    result_root_cid TEXT,
    artifacts JSONB,
    metrics JSONB
);

CREATE INDEX IF NOT EXISTS demand_jobs_status_created_idx
    ON demand_jobs (status, created_at DESC);

CREATE INDEX IF NOT EXISTS demand_jobs_lease_expires_idx
    ON demand_jobs (lease_expires_at);
