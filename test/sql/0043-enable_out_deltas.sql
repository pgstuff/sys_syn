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

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES (1,              'test_data_1'),
       (2,              'test_data_2');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');

UPDATE  sys_syn.out_tables_def
SET     enable_adds     = FALSE,
        enable_changes  = FALSE,
        enable_deletes  = FALSE
WHERE   in_table_id     = 'test_table' AND
        out_group_id    = 'out';

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT  out_queue.id,  out_queue.delta_type,   out_queue.queue_state,
        (in_data.id).*,        (in_data.attributes).*
FROM    user_data.test_table_out_queue_1 out_queue
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

UPDATE  sys_syn.out_tables_def
SET     enable_adds     = TRUE
WHERE   in_table_id     = 'test_table' AND
        out_group_id    = 'out';

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 2;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT  out_queue.id,  out_queue.delta_type,   out_queue.queue_state,
        (in_data.id).*,        (in_data.attributes).*
FROM    user_data.test_table_out_queue_1 out_queue
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id IN (1, 2);
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id IN (1, 2);
SELECT user_data.test_table_out_processed_1();

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_1 out_baseline
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

UPDATE user_data.test_table SET test_table_text = 'test_data_1 v2' WHERE test_table_id = 1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 3;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT  out_queue.id,  out_queue.delta_type,   out_queue.queue_state,
        (in_data.id).*,        (in_data.attributes).*
FROM    user_data.test_table_out_queue_1 out_queue
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

UPDATE  sys_syn.out_tables_def
SET     enable_changes  = TRUE
WHERE   in_table_id     = 'test_table' AND
        out_group_id    = 'out';

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 4;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT  out_queue.id,  out_queue.delta_type,   out_queue.queue_state,
        (in_data.id).*,        (in_data.attributes).*
FROM    user_data.test_table_out_queue_1 out_queue
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
SELECT user_data.test_table_out_processed_1();

DELETE FROM user_data.test_table WHERE test_table_id = 2;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 5;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT  out_queue.id,  out_queue.delta_type,   out_queue.queue_state,
        (in_data.id).*,        (in_data.attributes).*
FROM    user_data.test_table_out_queue_1 out_queue
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

UPDATE  sys_syn.out_tables_def
SET     enable_deletes  = TRUE
WHERE   in_table_id     = 'test_table' AND
        out_group_id    = 'out';

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 6;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT  out_queue.id,  out_queue.delta_type,   out_queue.queue_state,
        (in_data.id).*,        (in_data.attributes).*
FROM    user_data.test_table_out_queue_1 out_queue
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 2;
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 2;
SELECT user_data.test_table_out_processed_1();

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_1 out_baseline
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

ROLLBACK;
