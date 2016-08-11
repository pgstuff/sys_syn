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
VALUES (1,              'test_data v1');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out', out_log_lifetime => INTERVAL '3.85 days');

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();
UPDATE user_data.test_table_out_queue SET queue_state = 'Reading'::sys_syn.queue_state WHERE (key).test_table_key = 1;
UPDATE user_data.test_table_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (key).test_table_key = 1;
SELECT user_data.test_table_out_processed();

UPDATE user_data.test_table SET test_table_text = 'test_data v2' WHERE test_table_key = 1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();
UPDATE user_data.test_table_out_queue SET queue_state = 'Reading'::sys_syn.queue_state WHERE (key).test_table_key = 1;
UPDATE user_data.test_table_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (key).test_table_key = 1;
SELECT user_data.test_table_out_processed();

SELECT  out_baseline.key,
        (in_data.key).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline out_baseline
        LEFT JOIN user_data.test_table_in AS in_data USING (trans_id_in, key);

SELECT  test_table_out_log.trans_id_in,
        test_table_out_log.key,
        test_table_out_log.trans_id_out,
        CASE
                WHEN test_table_out_log.processed_time IS NULL
                        THEN 'NULL'
                ELSE 'NOT NULL'
        END AS processed_time_test,
        test_table_out_log.delta_type,
        test_table_out_log.queue_id
FROM    user_data.test_table_out_log
ORDER BY test_table_out_log.trans_id_out;

ROLLBACK;
