BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION pgcrypto SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in', in_partition_count => 2::smallint);

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in', in_partition_count => 2::smallint);
END$$;

INSERT INTO user_data.test_table(
        test_table_id,          test_table_text)
SELECT  generate_series,        'test_data ' || generate_series
FROM    generate_series(1, 10);

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');

ALTER TABLE user_data.test_table_out_queue_1
  ADD FOREIGN KEY (trans_id_in, id) REFERENCES user_data.test_table_in_1 (trans_id_in, id) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE user_data.test_table_out_queue_2
  ADD FOREIGN KEY (trans_id_in, id) REFERENCES user_data.test_table_in_2 (trans_id_in, id) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();
SELECT user_data.test_table_out_move_2();

SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue_1;
SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue_2;

SELECT user_data.test_table_vacuum_1();
SELECT user_data.test_table_vacuum_2();

UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_2 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;

UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_2 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;

SELECT user_data.test_table_out_processed_1();
SELECT user_data.test_table_out_processed_2();

SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue_1;
SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue_2;

SELECT user_data.test_table_vacuum_1();
SELECT user_data.test_table_vacuum_2();

ROLLBACK;
