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

SELECT sys_syn.in_table_add_sql('user_data.test_table'::regclass, 'in');

DO $$BEGIN
    EXECUTE sys_syn.in_table_add_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table(
        test_table_key, test_table_text)
VALUES (1,              'test_data');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out', data_view => TRUE);

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT * FROM user_data.test_table_out_queue_data;

UPDATE user_data.test_table_out_queue_data SET sys_syn_queue_state = 'Reading'::sys_syn.queue_state WHERE test_table_key = 1;

UPDATE user_data.test_table_out_queue_data SET sys_syn_queue_state = 'Processed'::sys_syn.queue_state WHERE test_table_key = 1;

SELECT user_data.test_table_out_processed();

SELECT * FROM user_data.test_table_out_queue_data;

ROLLBACK;
