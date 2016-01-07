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

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out');

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();
UPDATE user_data.test_table_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (key).test_table_key = 1;
SELECT user_data.test_table_out_processed();

SELECT  out_baseline.key,
        (in_data.key).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline out_baseline
        LEFT JOIN user_data.test_table_in AS in_data USING (trans_id_in, key);

DELETE FROM user_data.test_table WHERE test_table_key = 1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();
UPDATE user_data.test_table_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (key).test_table_key = 1;
SELECT user_data.test_table_out_processed();

SELECT  out_baseline.key,
        (in_data.key).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline out_baseline
        LEFT JOIN user_data.test_table_in AS in_data USING (trans_id_in, key);

ROLLBACK;
