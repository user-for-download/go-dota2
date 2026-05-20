-- Create an overload of ensure_future_time_partitions that accepts a text
-- interval string (e.g., '12 months') for manual/ad-hoc usage.
-- This coexists with the integer-based version used by the Go partition admin.
--
-- Naming convention: <table>_<year>_q<quarter> (lowercase q, e.g. matches_2026_q2)
-- Requires PostgreSQL 13+ (row triggers on partitioned parents).

CREATE OR REPLACE FUNCTION ensure_future_time_partitions(
    table_names text[],
    ahead       text
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    t        text;
    pname    text;
    qstart   timestamptz;
    qend     timestamptz;
    cutoff   timestamptz;
    yr       int;
    qr       int;
BEGIN
    -- Cast the text interval string (e.g., '12 months') to interval type
    cutoff := now() + ahead::interval;
    qstart := date_trunc('quarter', now());

    WHILE qstart < cutoff LOOP
        qend := qstart + INTERVAL '3 months';
        yr := EXTRACT(YEAR FROM qstart)::int;
        qr := ((EXTRACT(MONTH FROM qstart)::int - 1) / 3) + 1;

        FOREACH t IN ARRAY table_names LOOP
            -- Partition naming convention: <table>_<year>_q<quarter>
            -- e.g. matches_2026_q2, player_matches_2026_q3
            pname := format('%s_%s_q%s', t, yr, qr);

            IF NOT EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = CURRENT_SCHEMA()
                  AND c.relname = pname
            ) THEN
                EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                    pname, t,
                    EXTRACT(EPOCH FROM qstart)::bigint,
                    EXTRACT(EPOCH FROM qend)::bigint
                );

                -- Apply aggressive autovacuum for time-series tables
                IF t IN ('matches', 'player_matches', 'public_matches', 'player_timeseries') THEN
                    EXECUTE format(
                        'ALTER TABLE %I SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.01)',
                        pname
                    );
                END IF;
            END IF;
        END LOOP;

        qstart := qend;
    END LOOP;
END;
$$;

INSERT INTO schema_migrations (version, filename) VALUES (2, '002_ensure_future_time_partitions.sql')
ON CONFLICT (version) DO NOTHING;
