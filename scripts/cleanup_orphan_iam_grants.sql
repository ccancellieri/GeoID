-- One-off cleanup: orphan rows in iam.grants from the principal_id regression.
--
-- Bug (geoid pre-`dea5a51`, surfaced 2026-05-09 in review env):
--   IamService.get_effective_permissions constructed Principal(...) without
--   passing id=, so Principal's default_factory=uuid4 produced a fresh random
--   UUID on every call. The OIDC role-sync reconciler then called
--   grant_platform_role(principal_id=principal.id, ...) which inserted into
--   iam.grants against UUIDs that have no matching row in iam.principals.
--
-- Effect: rows in iam.grants where subject_kind='principal' and the
-- subject_ref UUID does not appear in iam.principals.id. Harmless — no auth
-- path can reach them because LIST_ROLE_NAMES_FOR_IDENTITY joins through
-- identity_links → principals — but they pollute the table and skew any
-- "role assignments per principal" reporting.
--
-- Run this once after deploying the dea5a51 fix. Safe to re-run (idempotent).
--
-- Usage:
--   psql "$DSN" -f scripts/cleanup_orphan_iam_grants.sql
--
-- Or via pgcron one-shot:
--   SELECT cron.schedule('cleanup-orphan-grants-once', '* * * * *', $$
--     <body of this file>;
--     SELECT cron.unschedule('cleanup-orphan-grants-once');
--   $$);

BEGIN;

-- 1. Snapshot what we're about to remove (for audit/log capture).
WITH orphans AS (
    SELECT id, subject_ref, object_kind, object_ref, scope_kind, scope_ref, created_at
    FROM iam.grants
    WHERE subject_kind = 'principal'
      AND subject_ref::uuid NOT IN (SELECT id FROM iam.principals)
)
SELECT
    COUNT(*)                              AS orphan_count,
    COUNT(DISTINCT subject_ref)           AS distinct_orphan_principals,
    COUNT(*) FILTER (WHERE object_kind = 'role') AS role_grants,
    MIN(created_at)                       AS earliest,
    MAX(created_at)                       AS latest
FROM orphans;

-- 2. Delete.
DELETE FROM iam.grants
WHERE subject_kind = 'principal'
  AND subject_ref::uuid NOT IN (SELECT id FROM iam.principals);

COMMIT;
