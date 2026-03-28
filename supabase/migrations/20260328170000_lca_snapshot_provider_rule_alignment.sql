BEGIN;

ALTER TABLE public.lca_network_snapshots
    ALTER COLUMN provider_matching_rule
    SET DEFAULT 'split_by_evidence_hybrid';

ALTER TABLE public.lca_network_snapshots
    DROP CONSTRAINT IF EXISTS lca_network_snapshots_provider_rule_chk;

ALTER TABLE public.lca_network_snapshots
    ADD CONSTRAINT lca_network_snapshots_provider_rule_chk
    CHECK (provider_matching_rule IN (
        'strict_unique_provider',
        'best_provider_strict',
        'split_by_evidence',
        'split_by_evidence_hybrid',
        'split_equal',
        'equal_split_multi_provider',
        'custom_weighted_provider'
    ));

COMMIT;
