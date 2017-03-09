BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

DO $$BEGIN
    EXECUTE sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');

SELECT sys_syn.in_pull_sequence_populate_assume();

SELECT sys_syn.jobs_get_crontab();

ROLLBACK;
