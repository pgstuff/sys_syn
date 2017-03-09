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
VALUES (1,              'test_data v1');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
SELECT user_data.test_table_out_processed_1();

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_1 AS out_baseline
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

UPDATE user_data.test_table SET test_table_text = 'test_data v2' WHERE test_table_id = 1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 2;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
SELECT user_data.test_table_out_processed_1();

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_1 AS out_baseline
        LEFT JOIN user_data.test_table_in_1 in_data USING (trans_id_in, id);

SELECT  in_data.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_in_1 AS in_data;

SELECT user_data.test_table_vacuum_1();

SELECT  in_data.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_in_1 AS in_data;

ROLLBACK;
