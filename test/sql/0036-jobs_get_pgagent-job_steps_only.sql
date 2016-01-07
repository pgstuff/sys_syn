BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data AUTHORIZATION postgres;

CREATE TABLE user_data.in_1_test_table (
        in_1_test_table_key integer NOT NULL,
        in_1_test_table_text text,
        CONSTRAINT in_1_test_table_pkey PRIMARY KEY (in_1_test_table_key));

CREATE TABLE user_data.in_2_test_table (
        in_2_test_table_key integer NOT NULL,
        in_2_test_table_text text,
        CONSTRAINT in_2_test_table_pkey PRIMARY KEY (in_2_test_table_key));

INSERT INTO sys_syn.in_groups_def VALUES ('in_1');

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql('user_data.in_1_test_table'::regclass, 'in_1');
END$$;

INSERT INTO sys_syn.in_groups_def VALUES ('in_2');

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql('user_data.in_2_test_table'::regclass, 'in_2');
END$$;

INSERT INTO sys_syn.out_groups_def VALUES ('out_1');

SELECT sys_syn.out_table_add('user_data', 'in_1_test_table', 'out_1');
SELECT sys_syn.out_table_add('user_data', 'in_2_test_table', 'out_1');

INSERT INTO sys_syn.out_groups_def VALUES ('out_2');

SELECT sys_syn.out_table_add('user_data', 'in_1_test_table', 'out_2');
SELECT sys_syn.out_table_add('user_data', 'in_2_test_table', 'out_2');

SELECT sys_syn.in_pull_sequence_populate_assume();

SELECT sys_syn.jobs_get_pgagent(delete_sql => true, create_sql => true, job_steps_only => true);

ROLLBACK;
