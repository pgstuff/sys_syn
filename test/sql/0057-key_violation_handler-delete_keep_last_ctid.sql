BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text text);

INSERT INTO sys_syn.in_groups_def VALUES ('in');

DO $$BEGIN
        EXECUTE sys_syn.prepull_create_sql('user_data.test_table'::regclass, 'in', id_columns => ARRAY['test_table_id']);
END$$;

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql(
                relation        => 'user_data.test_table_prepull_full'::regclass,
                in_group_id     => 'in',
                schema          => 'user_data',
                id_columns      => ARRAY['test_table_id'],
                no_diff_columns => NULL,
                omit_columns    => ARRAY[]::TEXT[],
                limit_to_columns=> NULL,
                full_prepull_id => 'test_table',
                changes_prepull_id=> NULL,
                in_table_id     => 'test_table',
                key_violation_handler => 'delete_keep_last_ctid'::sys_syn.key_violation_handler);
END$$;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES (1,              'test_data 1'),(
        1,              'test_data 1'),(
        2,              'test_data 2 first'),(
        2,              'test_data 2 middle'),(
        2,              'test_data 2 last'),(
        3,              'test_data 3');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');

SELECT user_data.test_table_prepull_full();
UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 2;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();


SELECT  (in_data.id).*,
        out_queue.delta_type,
        out_queue.queue_state,
        (in_data.attributes).*
FROM    user_data.test_table_out_queue_1 out_queue
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id)
ORDER BY in_data.id;

ROLLBACK;
