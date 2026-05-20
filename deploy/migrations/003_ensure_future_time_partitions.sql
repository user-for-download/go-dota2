-- =====================================================
-- 003_ensure_future_time_partitions.sql
-- Create the ensure_future_time_partitions function for
-- automatic quarterly partition management of time-series
-- match tables.
-- =====================================================

CREATE OR REPLACE FUNCTION ensure_future_time_partitions(
    table_names text[],
    days_ahead  integer
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    t        text;
    pname    text;
    qstart   timestamptz;
    qend     timestamptz;
    cutoff   timestamptz;
    lo       bigint;
    hi       bigint;
BEGIN
    cutoff := now() + (days_ahead || ' days')::interval;
    qstart := date_trunc('quarter', now());

    WHILE qstart < cutoff LOOP
        qend := qstart + INTERVAL '3 months';
        -- Convert timestamptz boundaries to Unix epoch seconds
        -- to match the RANGE partition bounds on start_time (BIGINT).
        lo := EXTRACT(EPOCH FROM qstart)::bigint;
        hi := EXTRACT(EPOCH FROM qend)::bigint;

        FOREACH t IN ARRAY table_names LOOP
            -- Partition naming convention: <table>_YYYY_Qn
            -- e.g. matches_2026_q2, player_matches_2026_q3
            pname := t || '_' || to_char(qstart, 'YYYY"_Q"Q');

            IF NOT EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = CURRENT_SCHEMA()
                  AND c.relname = pname
            ) THEN
                EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)',
                    pname, t, lo, hi
                );
            END IF;
        END LOOP;

        qstart := qend;
    END LOOP;
END;
$$;
