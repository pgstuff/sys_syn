BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.pre_pull_add_sql('user_data.test_table'::regclass, 'in');

DO $$BEGIN
    EXECUTE sys_syn.pre_pull_add_sql('user_data.test_table'::regclass, 'in');
END$$;

ROLLBACK;
