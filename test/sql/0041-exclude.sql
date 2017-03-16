BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

INSERT INTO sys_syn.exclude_reasons(
        exclude_reason_id,      exclude_code,           comments)
VALUES (1001,                   'custom_exclude',       'Test');

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

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES (1,              'Exclude from in'),
       (2,              'Exclude from out'),
       (3,              'test_data');

INSERT INTO user_data.test_table_exclude VALUES (ROW(1), 1001);

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');

INSERT INTO user_data.test_table_out_exclude_1 VALUES (ROW(2), 1001);

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue_1;

SELECT user_data.test_table_vacuum_1();

SELECT * FROM user_data.test_table_in_1;

ROLLBACK;
