-- Phase 1 additive-only migration for full-library LCA snapshot workflow.
-- Safety contract:
--   * CREATE-only objects in public.lca_* namespace
--   * No ALTER/DROP/UPDATE/DELETE/TRUNCATE on existing business tables

BEGIN;

-- 1) Snapshot header
CREATE TABLE IF NOT EXISTS public.lca_network_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    scope text NOT NULL DEFAULT 'full_library',
    process_filter jsonb,
    lcia_method_id uuid,
    lcia_method_version char(9),
    provider_matching_rule text NOT NULL DEFAULT 'strict_unique_provider',
    source_hash text,
    status text NOT NULL DEFAULT 'draft',
    created_by uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT lca_network_snapshots_scope_chk
        CHECK (scope IN ('full_library')),
    CONSTRAINT lca_network_snapshots_provider_rule_chk
        CHECK (provider_matching_rule IN (
            'strict_unique_provider',
            'equal_split_multi_provider',
            'custom_weighted_provider'
        )),
    CONSTRAINT lca_network_snapshots_status_chk
        CHECK (status IN ('draft', 'ready', 'stale', 'failed')),
    CONSTRAINT lca_network_snapshots_lcia_fk
        FOREIGN KEY (lcia_method_id, lcia_method_version)
        REFERENCES public.lciamethods (id, version)
        ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS lca_network_snapshots_status_created_idx
    ON public.lca_network_snapshots (status, created_at DESC);

CREATE INDEX IF NOT EXISTS lca_network_snapshots_updated_idx
    ON public.lca_network_snapshots (updated_at DESC);

-- 2) Process dimension index
CREATE TABLE IF NOT EXISTS public.lca_process_index (
    snapshot_id uuid NOT NULL,
    process_idx integer NOT NULL,
    process_id uuid NOT NULL,
    process_version char(9) NOT NULL,
    state_code integer,
    PRIMARY KEY (snapshot_id, process_idx),
    CONSTRAINT lca_process_index_snapshot_fk
        FOREIGN KEY (snapshot_id)
        REFERENCES public.lca_network_snapshots (id)
        ON DELETE CASCADE,
    CONSTRAINT lca_process_index_process_unique
        UNIQUE (snapshot_id, process_id, process_version)
);

CREATE INDEX IF NOT EXISTS lca_process_index_process_lookup_idx
    ON public.lca_process_index (snapshot_id, process_id, process_version);

-- 3) Flow dimension index
CREATE TABLE IF NOT EXISTS public.lca_flow_index (
    snapshot_id uuid NOT NULL,
    flow_idx integer NOT NULL,
    flow_id uuid NOT NULL,
    flow_version char(9),
    flow_kind text NOT NULL DEFAULT 'unknown',
    PRIMARY KEY (snapshot_id, flow_idx),
    CONSTRAINT lca_flow_index_snapshot_fk
        FOREIGN KEY (snapshot_id)
        REFERENCES public.lca_network_snapshots (id)
        ON DELETE CASCADE,
    CONSTRAINT lca_flow_index_flow_kind_chk
        CHECK (flow_kind IN ('elementary', 'product', 'unknown'))
);

CREATE INDEX IF NOT EXISTS lca_flow_index_flow_lookup_idx
    ON public.lca_flow_index (snapshot_id, flow_id);

-- 4) A matrix entries
CREATE TABLE IF NOT EXISTS public.lca_technosphere_entries (
    snapshot_id uuid NOT NULL,
    row integer NOT NULL,
    col integer NOT NULL,
    value double precision NOT NULL,
    input_process_idx integer,
    provider_process_idx integer,
    flow_id uuid,
    PRIMARY KEY (snapshot_id, row, col),
    CONSTRAINT lca_technosphere_snapshot_fk
        FOREIGN KEY (snapshot_id)
        REFERENCES public.lca_network_snapshots (id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS lca_technosphere_col_idx
    ON public.lca_technosphere_entries (snapshot_id, col);

CREATE INDEX IF NOT EXISTS lca_technosphere_row_idx
    ON public.lca_technosphere_entries (snapshot_id, row);

-- 5) B matrix entries
CREATE TABLE IF NOT EXISTS public.lca_biosphere_entries (
    snapshot_id uuid NOT NULL,
    row integer NOT NULL,
    col integer NOT NULL,
    value double precision NOT NULL,
    flow_id uuid,
    process_idx integer,
    PRIMARY KEY (snapshot_id, row, col),
    CONSTRAINT lca_biosphere_snapshot_fk
        FOREIGN KEY (snapshot_id)
        REFERENCES public.lca_network_snapshots (id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS lca_biosphere_col_idx
    ON public.lca_biosphere_entries (snapshot_id, col);

CREATE INDEX IF NOT EXISTS lca_biosphere_row_idx
    ON public.lca_biosphere_entries (snapshot_id, row);

-- 6) C matrix entries
CREATE TABLE IF NOT EXISTS public.lca_characterization_factors (
    snapshot_id uuid NOT NULL,
    row integer NOT NULL,
    col integer NOT NULL,
    value double precision NOT NULL,
    method_id uuid,
    method_version char(9),
    indicator_key text,
    PRIMARY KEY (snapshot_id, row, col),
    CONSTRAINT lca_cf_snapshot_fk
        FOREIGN KEY (snapshot_id)
        REFERENCES public.lca_network_snapshots (id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS lca_cf_col_idx
    ON public.lca_characterization_factors (snapshot_id, col);

CREATE INDEX IF NOT EXISTS lca_cf_method_idx
    ON public.lca_characterization_factors (method_id, method_version);

-- 7) Async jobs
CREATE TABLE IF NOT EXISTS public.lca_jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type text NOT NULL,
    snapshot_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'queued',
    payload jsonb,
    diagnostics jsonb,
    attempt integer NOT NULL DEFAULT 0,
    max_attempt integer NOT NULL DEFAULT 3,
    requested_by uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT lca_jobs_snapshot_fk
        FOREIGN KEY (snapshot_id)
        REFERENCES public.lca_network_snapshots (id)
        ON DELETE CASCADE,
    CONSTRAINT lca_jobs_type_chk
        CHECK (job_type IN (
            'prepare_factorization',
            'solve_one',
            'solve_batch',
            'invalidate_factorization',
            'rebuild_factorization'
        )),
    CONSTRAINT lca_jobs_status_chk
        CHECK (status IN ('queued', 'running', 'ready', 'completed', 'failed', 'stale')),
    CONSTRAINT lca_jobs_attempt_chk
        CHECK (attempt >= 0 AND max_attempt >= 0 AND attempt <= max_attempt)
);

CREATE INDEX IF NOT EXISTS lca_jobs_status_created_idx
    ON public.lca_jobs (status, created_at);

CREATE INDEX IF NOT EXISTS lca_jobs_snapshot_created_idx
    ON public.lca_jobs (snapshot_id, created_at DESC);

-- 8) Results (hybrid payload + artifact reference)
CREATE TABLE IF NOT EXISTS public.lca_results (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id uuid NOT NULL,
    snapshot_id uuid NOT NULL,
    payload jsonb,
    diagnostics jsonb,
    artifact_url text,
    artifact_sha256 text,
    artifact_byte_size bigint,
    artifact_format text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT lca_results_job_fk
        FOREIGN KEY (job_id)
        REFERENCES public.lca_jobs (id)
        ON DELETE CASCADE,
    CONSTRAINT lca_results_snapshot_fk
        FOREIGN KEY (snapshot_id)
        REFERENCES public.lca_network_snapshots (id)
        ON DELETE CASCADE,
    CONSTRAINT lca_results_artifact_size_chk
        CHECK (artifact_byte_size IS NULL OR artifact_byte_size >= 0)
);

CREATE INDEX IF NOT EXISTS lca_results_job_idx
    ON public.lca_results (job_id);

CREATE INDEX IF NOT EXISTS lca_results_snapshot_created_idx
    ON public.lca_results (snapshot_id, created_at DESC);

-- 9) Dedicated queue for LCA worker (additive-only in pgmq metadata)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pgmq.meta WHERE queue_name = 'lca_jobs'
    ) THEN
        PERFORM pgmq.create('lca_jobs');
    END IF;
END
$$;

COMMIT;
