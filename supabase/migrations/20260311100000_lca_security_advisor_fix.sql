begin;

-- Fix: Security Advisor warning "Function Search Path Mutable"
alter function public.policy_is_current_user_in_roles(uuid, text[])
set search_path = public, pg_temp;

-- Fix: Security Advisor suggestion "RLS Enabled No Policy" on LCA system tables
DO $$
DECLARE
  t text;
  policy_name text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'lca_active_snapshots',
    'lca_factorization_registry',
    'lca_latest_all_unit_results',
    'lca_network_snapshots',
    'lca_result_cache',
    'lca_snapshot_artifacts'
  ] LOOP
    policy_name := t || '_service_role_all';

    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS %I ON public.%I', policy_name, t);
    EXECUTE format(
      'CREATE POLICY %I ON public.%I FOR ALL TO service_role USING (true) WITH CHECK (true)',
      policy_name,
      t
    );
  END LOOP;
END $$;

commit;
