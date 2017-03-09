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

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');

DO $$BEGIN
    EXECUTE sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES (1,              'test_data 1');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out', data_view => TRUE);

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT * FROM user_data.test_table_out_queue_data_1;

ALTER TABLE user_data.test_table
        ADD COLUMN test_table_date date,
        ADD COLUMN test_table_datetime timestamp with time zone;

SELECT sys_syn.in_table_columns_add_sql('test_table');

DO $$BEGIN
    EXECUTE sys_syn.in_table_columns_add_sql('test_table', 'user_data.test_table'::regclass);
END$$;

SELECT * FROM user_data.test_table_out_queue_data_1;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text,        test_table_date,        test_table_datetime)
VALUES (2,              'test_data 2',          '6283-01-08',           '6283-01-08 05:30:07');

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 2;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT * FROM user_data.test_table_out_queue_data_1;

ROLLBACK;
