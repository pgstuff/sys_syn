BEGIN;

DO $$BEGIN
        IF EXISTS (SELECT FROM pg_settings WHERE name = 'server_version_num' AND setting::INT < 90500) THEN
                RAISE EXCEPTION 'PostgreSQL 9.5 or above is required.  You are using version %.',
                        (SELECT setting FROM pg_settings WHERE name = 'server_version')
                        USING HINT = 'Upgrade this PostgreSQL instance or add another instance with the current version.';
        END IF;
END$$;

CREATE EXTENSION tinyint
        SCHEMA public;

ROLLBACK;
