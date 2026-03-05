-- Lock down lca_* tables with RLS and least privilege grants.
-- Additive-only security migration.

BEGIN;

-- Revoke broad default grants from client roles.
REVOKE ALL PRIVILEGES ON TABLE public.lca_active_snapshots FROM anon, authenticated;
REVOKE ALL PRIVILEGES ON TABLE public.lca_factorization_registry FROM anon, authenticated;
REVOKE ALL PRIVILEGES ON TABLE public.lca_jobs FROM anon, authenticated;
REVOKE ALL PRIVILEGES ON TABLE public.lca_network_snapshots FROM anon, authenticated;
REVOKE ALL PRIVILEGES ON TABLE public.lca_result_cache FROM anon, authenticated;
REVOKE ALL PRIVILEGES ON TABLE public.lca_results FROM anon, authenticated;
REVOKE ALL PRIVILEGES ON TABLE public.lca_snapshot_artifacts FROM anon, authenticated;

-- Keep service_role explicit privileges.
GRANT ALL PRIVILEGES ON TABLE public.lca_active_snapshots TO service_role;
GRANT ALL PRIVILEGES ON TABLE public.lca_factorization_registry TO service_role;
GRANT ALL PRIVILEGES ON TABLE public.lca_jobs TO service_role;
GRANT ALL PRIVILEGES ON TABLE public.lca_network_snapshots TO service_role;
GRANT ALL PRIVILEGES ON TABLE public.lca_result_cache TO service_role;
GRANT ALL PRIVILEGES ON TABLE public.lca_results TO service_role;
GRANT ALL PRIVILEGES ON TABLE public.lca_snapshot_artifacts TO service_role;

-- Enable row level security on all lca_* runtime tables.
ALTER TABLE public.lca_active_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.lca_factorization_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.lca_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.lca_network_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.lca_result_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.lca_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.lca_snapshot_artifacts ENABLE ROW LEVEL SECURITY;

-- Authenticated users can only read their own jobs.
GRANT SELECT ON TABLE public.lca_jobs TO authenticated;
DROP POLICY IF EXISTS lca_jobs_select_own ON public.lca_jobs;
CREATE POLICY lca_jobs_select_own
    ON public.lca_jobs
    FOR SELECT
    TO authenticated
    USING (requested_by = auth.uid());

-- Authenticated users can only read results linked to their own jobs.
GRANT SELECT ON TABLE public.lca_results TO authenticated;
DROP POLICY IF EXISTS lca_results_select_own ON public.lca_results;
CREATE POLICY lca_results_select_own
    ON public.lca_results
    FOR SELECT
    TO authenticated
    USING (
        EXISTS (
            SELECT 1
            FROM public.lca_jobs AS j
            WHERE j.id = public.lca_results.job_id
              AND j.requested_by = auth.uid()
        )
    );

COMMIT;
