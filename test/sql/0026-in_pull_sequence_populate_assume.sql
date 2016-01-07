BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_key integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pkey PRIMARY KEY (test_table_key));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

DO $$BEGIN
    EXECUTE sys_syn.in_table_add_sql('user_data.test_table'::regclass, 'in');
END$$;

SELECT sys_syn.in_pull_sequence_populate_assume();

SELECT * FROM sys_syn.in_pull_sequences_def;

SELECT * FROM sys_syn.in_pull_sequence_pulls;

ROLLBACK;
