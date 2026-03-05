-- Add additive-only schema for active snapshot pointer, request/result cache,
-- factorization registry, and job idempotency keys.

BEGIN;

CREATE TABLE IF NOT EXISTS public.lca_active_snapshots (
    scope text PRIMARY KEY,
    snapshot_id uuid NOT NULL,
    source_hash text NOT NULL,
    activated_at timestamptz NOT NULL DEFAULT now(),
    activated_by uuid,
    note text,
    CONSTRAINT lca_active_snapshots_snapshot_fk
        FOREIGN KEY (snapshot_id)
        REFERENCES public.lca_network_snapshots (id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS lca_active_snapshots_snapshot_idx
    ON public.lca_active_snapshots (snapshot_id);

CREATE TABLE IF NOT EXISTS public.lca_result_cache (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    scope text NOT NULL DEFAULT 'prod',
    snapshot_id uuid NOT NULL,
    request_key text NOT NULL,
    request_payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    job_id uuid,
    result_id uuid,
    error_code text,
    error_message text,
    hit_count bigint NOT NULL DEFAULT 0,
    last_accessed_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT lca_result_cache_snapshot_fk
        FOREIGN KEY (snapshot_id)
        REFERENCES public.lca_network_snapshots (id)
        ON DELETE CASCADE,
    CONSTRAINT lca_result_cache_job_fk
        FOREIGN KEY (job_id)
        REFERENCES public.lca_jobs (id)
        ON DELETE SET NULL,
    CONSTRAINT lca_result_cache_result_fk
        FOREIGN KEY (result_id)
        REFERENCES public.lca_results (id)
        ON DELETE SET NULL,
    CONSTRAINT lca_result_cache_status_chk
        CHECK (status IN ('pending', 'running', 'ready', 'failed', 'stale')),
    CONSTRAINT lca_result_cache_hit_count_chk
        CHECK (hit_count >= 0),
    CONSTRAINT lca_result_cache_request_key_chk
        CHECK (length(request_key) > 0),
    CONSTRAINT lca_result_cache_scope_snapshot_request_key_uk
        UNIQUE (scope, snapshot_id, request_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS lca_result_cache_job_uidx
    ON public.lca_result_cache (job_id)
    WHERE job_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS lca_result_cache_result_uidx
    ON public.lca_result_cache (result_id)
    WHERE result_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS lca_result_cache_lookup_idx
    ON public.lca_result_cache (scope, snapshot_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS lca_result_cache_last_accessed_idx
    ON public.lca_result_cache (last_accessed_at DESC);

CREATE TABLE IF NOT EXISTS public.lca_factorization_registry (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    scope text NOT NULL DEFAULT 'prod',
    snapshot_id uuid NOT NULL,
    backend text NOT NULL DEFAULT 'umfpack',
    numeric_options_hash text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    owner_worker_id text,
    lease_until timestamptz,
    prepared_job_id uuid,
    diagnostics jsonb NOT NULL DEFAULT '{}'::jsonb,
    prepared_at timestamptz,
    last_used_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT lca_factorization_registry_snapshot_fk
        FOREIGN KEY (snapshot_id)
        REFERENCES public.lca_network_snapshots (id)
        ON DELETE CASCADE,
    CONSTRAINT lca_factorization_registry_prepared_job_fk
        FOREIGN KEY (prepared_job_id)
        REFERENCES public.lca_jobs (id)
        ON DELETE SET NULL,
    CONSTRAINT lca_factorization_registry_status_chk
        CHECK (status IN ('pending', 'building', 'ready', 'failed', 'stale')),
    CONSTRAINT lca_factorization_registry_backend_chk
        CHECK (backend IN ('umfpack', 'cholmod', 'spqr')),
    CONSTRAINT lca_factorization_registry_scope_snapshot_backend_opts_uk
        UNIQUE (scope, snapshot_id, backend, numeric_options_hash)
);

CREATE INDEX IF NOT EXISTS lca_factorization_registry_status_lease_idx
    ON public.lca_factorization_registry (status, lease_until);

CREATE INDEX IF NOT EXISTS lca_factorization_registry_snapshot_status_idx
    ON public.lca_factorization_registry (snapshot_id, status, updated_at DESC);

ALTER TABLE public.lca_jobs
    ADD COLUMN IF NOT EXISTS request_key text;

ALTER TABLE public.lca_jobs
    ADD COLUMN IF NOT EXISTS idempotency_key text;

CREATE UNIQUE INDEX IF NOT EXISTS lca_jobs_idempotency_key_uidx
    ON public.lca_jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS lca_jobs_snapshot_type_status_created_idx
    ON public.lca_jobs (snapshot_id, job_type, status, created_at DESC);

COMMIT;
