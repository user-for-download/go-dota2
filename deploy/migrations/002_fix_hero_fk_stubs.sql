-- =====================================================
-- 002_fix_hero_fk_stubs.sql
-- Purpose: Prevent FK violations on hero_id when the parser
--          ingests matches referencing heroes not yet loaded
--          by the enricher (new patch heroes, e.g. ids 228, 240).
--
-- Strategy:
--   1. Provide a helper to upsert hero stubs in bulk (used by
--      the ingester before staging->final inserts).
--   2. Install BEFORE INSERT triggers on draft_timings,
--      picks_bans, and player_matches as a defense-in-depth
--      safety net.
--   3. Ensure the enricher's hero loader overwrites stubs
--      (documented in COMMENT; loader must use ON CONFLICT
--      DO UPDATE — no schema change needed here).
--   4. Backfill any hero_ids already referenced in staging
--      tables, if present.
-- =====================================================

BEGIN;

-- -----------------------------------------------------
-- 1. Bulk stub upsert helper
-- -----------------------------------------------------
-- Usage from the ingester (preferred path):
--   SELECT ensure_hero_stubs(ARRAY(SELECT DISTINCT hero_id
--                                  FROM _stage_draft_timings
--                                  WHERE hero_id IS NOT NULL));
-- Set-based, single statement, safe under concurrency.
-- -----------------------------------------------------
CREATE OR REPLACE FUNCTION ensure_hero_stubs(p_hero_ids SMALLINT[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
inserted INTEGER;
BEGIN
    IF p_hero_ids IS NULL OR array_length(p_hero_ids, 1) IS NULL THEN
        RETURN 0;
END IF;

WITH ins AS (
INSERT INTO heroes (id, name, localized_name)
SELECT DISTINCT hid,
                'unknown_' || hid::text,
    'Unknown Hero ' || hid::text
FROM unnest(p_hero_ids) AS t(hid)
WHERE hid IS NOT NULL
    ON CONFLICT (id) DO NOTHING
        RETURNING 1
    )
SELECT COUNT(*) INTO inserted FROM ins;

RETURN inserted;
END;
$$;

COMMENT ON FUNCTION ensure_hero_stubs(SMALLINT[]) IS
'Inserts stub rows into heroes for any unknown hero_id. '
'Enricher must overwrite stubs via ON CONFLICT (id) DO UPDATE '
'on its next /heroes run.';

-- -----------------------------------------------------
-- 2. Row-level trigger safety net
-- -----------------------------------------------------
-- Fires only when hero_id is NOT NULL and not already present.
-- Adds minor per-row overhead but guarantees correctness even
-- if a loader path forgets to call ensure_hero_stubs() first.
-- -----------------------------------------------------
CREATE OR REPLACE FUNCTION trg_ensure_hero_stub()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.hero_id IS NOT NULL THEN
        INSERT INTO heroes (id, name, localized_name)
        VALUES (NEW.hero_id,
                'unknown_' || NEW.hero_id::text,
                'Unknown Hero ' || NEW.hero_id::text)
        ON CONFLICT (id) DO NOTHING;
END IF;
RETURN NEW;
END;
$$;

COMMENT ON FUNCTION trg_ensure_hero_stub() IS
'BEFORE INSERT trigger: auto-creates hero stubs to prevent FK '
'violations when parser ingests matches with new hero_ids.';

-- Attach triggers to all FK-bound tables.
-- Note: For partitioned parents, the trigger propagates to all partitions.
DO $$
DECLARE
t TEXT;
    tbls TEXT[] := ARRAY['draft_timings', 'picks_bans', 'player_matches'];
BEGIN
    FOREACH t IN ARRAY tbls LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_hero_stub ON %I', t, t);
EXECUTE format(
        'CREATE TRIGGER trg_%1$s_hero_stub
         BEFORE INSERT ON %1$I
         FOR EACH ROW
         EXECUTE FUNCTION trg_ensure_hero_stub()',
        t
        );
END LOOP;
END $$;

-- -----------------------------------------------------
-- 3. Backfill: pre-create stubs for any hero_ids already
--    referenced in staging tables (if they exist).
--    Safe no-op if staging tables don't exist yet.
-- -----------------------------------------------------
DO $$
DECLARE
staging_tables TEXT[] := ARRAY[
        '_stage_draft_timings',
        '_stage_picks_bans',
        '_stage_player_matches'
    ];
    s TEXT;
    cnt INTEGER;
BEGIN
    FOREACH s IN ARRAY staging_tables LOOP
        IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = s
        ) THEN
            EXECUTE format(
                'SELECT ensure_hero_stubs(ARRAY(
                    SELECT DISTINCT hero_id::smallint
                    FROM %I
                    WHERE hero_id IS NOT NULL
                ))', s
            ) INTO cnt;
            RAISE NOTICE 'Backfilled % hero stubs from %', cnt, s;
END IF;
END LOOP;
END $$;

-- -----------------------------------------------------
-- 4a. Index for monitoring view (name LIKE 'unknown\_%')
-- B-tree with text pattern match; leading constant prefix
-- 'unknown_' makes it indexable.
-- -----------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_heroes_name_unknown
    ON heroes (name)
    WHERE name LIKE 'unknown\_%' ESCAPE '\';

COMMENT ON INDEX idx_heroes_name_unknown IS
'Speeds v_unknown_heroes view and ad-hoc queries for stub heroes.';

-- -----------------------------------------------------
-- 4b. Monitoring view: surface heroes still in stub state
-- -----------------------------------------------------
CREATE OR REPLACE VIEW v_unknown_heroes AS
SELECT id,
       name,
       localized_name,
       updated_at,
       NOW() - updated_at AS age
FROM heroes
WHERE name LIKE 'unknown\_%' ESCAPE '\'
ORDER BY id;

COMMENT ON VIEW v_unknown_heroes IS
'Heroes inserted as stubs by the ingester/trigger. '
'Alert if any row has age > ~1h: enricher /heroes job is lagging.';

-- -----------------------------------------------------
-- 5. Record migration
-- -----------------------------------------------------
INSERT INTO schema_migrations (version, filename)
VALUES (2, '002_fix_hero_fk_stubs.sql')
    ON CONFLICT (version) DO NOTHING;

COMMIT;