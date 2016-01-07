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

INSERT INTO user_data.test_table(
        test_table_key, test_table_text)
VALUES (1,              'Exclude from in'),
       (2,              'Exclude from out'),
       (3,              'test_data');

INSERT INTO user_data.test_table_exclude VALUES (ROW(1));

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out');

INSERT INTO user_data.test_table_out_exclude VALUES (ROW(2));

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT key, delta_type, queue_state FROM user_data.test_table_out_queue;

SELECT user_data.test_table_vacuum();

SELECT * FROM user_data.test_table_in;

ROLLBACK;
