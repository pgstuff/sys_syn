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
        EXECUTE sys_syn.pre_pull_add_sql('user_data.test_table'::regclass, 'in');
END$$;

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql(
                relation        => 'user_data.test_table_prepull_full'::regclass,
                in_group_id     => 'in',
                schema          => 'user_data',
                key_columns     => ARRAY['test_table_key'],
                no_diff_columns => NULL,
                omit_columns    => ARRAY[]::TEXT[],
                limit_to_columns=> NULL,
                full_prepull_id => 'test_table',
                changes_prepull_id=> NULL,
                in_table_id     => 'test_table');
END$$;

INSERT INTO user_data.test_table(
        test_table_key, test_table_text)
VALUES (1,              'test_data');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out');

ALTER TABLE user_data.test_table_out_queue
  ADD FOREIGN KEY (trans_id_in, key) REFERENCES user_data.test_table_in (trans_id_in, key) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.test_table_prepull_full();
UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT key, delta_type, queue_state FROM user_data.test_table_out_queue;

ROLLBACK;
