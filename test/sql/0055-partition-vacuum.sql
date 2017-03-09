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

SELECT sys_syn.in_table_create_sql(
        'user_data.test_table'::regclass,
        'in',
        in_partitions => ARRAY[
                ('',NULL),
                ('',NULL),
                ('',NULL),
                ('',NULL)]::sys_syn.create_in_partition[]
);

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql(
                'user_data.test_table'::regclass,
                'in',
                in_partitions => ARRAY[
                        ('',NULL),
                        ('',NULL),
                        ('',NULL),
                        ('',NULL)]::sys_syn.create_in_partition[]
        );
END$$;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES (1,              'test_data');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();
SELECT user_data.test_table_out_move_2();
SELECT user_data.test_table_out_move_3();
SELECT user_data.test_table_out_move_4();
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_2 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_3 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_4 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_2 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_3 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_4 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
SELECT user_data.test_table_out_processed_1();

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_1 AS out_baseline
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_2 AS out_baseline
        LEFT JOIN user_data.test_table_in_2 AS in_data USING (trans_id_in, id);

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_3 AS out_baseline
        LEFT JOIN user_data.test_table_in_3 AS in_data USING (trans_id_in, id);

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_4 AS out_baseline
        LEFT JOIN user_data.test_table_in_4 AS in_data USING (trans_id_in, id);

DELETE FROM user_data.test_table WHERE test_table_id = 1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 2;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();
SELECT user_data.test_table_out_move_2();
SELECT user_data.test_table_out_move_3();
SELECT user_data.test_table_out_move_4();
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_2 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_3 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_4 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_2 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_3 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue_4 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
SELECT user_data.test_table_out_processed_1();
SELECT user_data.test_table_out_processed_2();
SELECT user_data.test_table_out_processed_3();
SELECT user_data.test_table_out_processed_4();

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_1 AS out_baseline
        LEFT JOIN user_data.test_table_in_1 in_data USING (trans_id_in, id);

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_2 AS out_baseline
        LEFT JOIN user_data.test_table_in_2 in_data USING (trans_id_in, id);

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_3 AS out_baseline
        LEFT JOIN user_data.test_table_in_3 in_data USING (trans_id_in, id);

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline_4 AS out_baseline
        LEFT JOIN user_data.test_table_in_4 in_data USING (trans_id_in, id);

SELECT  in_data.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_in_1 AS in_data;

SELECT  in_data.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_in_2 AS in_data;

SELECT  in_data.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_in_3 AS in_data;

SELECT  in_data.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_in_4 AS in_data;

SELECT user_data.test_table_vacuum_1();
SELECT user_data.test_table_vacuum_2();
SELECT user_data.test_table_vacuum_3();
SELECT user_data.test_table_vacuum_4();

SELECT  in_data.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_in_1 AS in_data;

SELECT  in_data.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_in_2 AS in_data;

SELECT  in_data.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_in_3 AS in_data;

SELECT  in_data.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_in_4 AS in_data;

ROLLBACK;
