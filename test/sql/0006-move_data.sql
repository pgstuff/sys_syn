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

SELECT sys_syn.in_table_add_sql('user_data.test_table'::regclass, 'in');

DO $$BEGIN
    EXECUTE sys_syn.in_table_add_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES (1,              'test_data');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out');

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue;

SELECT user_data.test_table_vacuum();

ROLLBACK;
