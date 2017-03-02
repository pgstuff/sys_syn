BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION pgcrypto SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text text,
        no_diff_text text,
        delete_row_indicator boolean DEFAULT FALSE,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

INSERT INTO sys_syn.in_column_transforms (
        rule_group_id,        priority,       data_type_like, relation_name_like,     column_name_like,
        in_column_type, new_data_type,  new_in_column_type,     omit,   new_column_name,
        final_rule)
VALUES (null,           50,             null,           null,                   'no_diff_text',
        null,           null,           'NoDiff',               null,   null,
        FALSE);

INSERT INTO sys_syn.in_column_transforms (
        rule_group_id,        priority,       data_type_like, relation_name_like,     column_name_like,
        in_column_type, new_data_type,  new_in_column_type,     omit,   new_column_name,
        final_rule)
VALUES (null,           50,             'boolean',      null,                   'delete_row_indicator',
        'Attribute',    null,           'NoDiff',               FALSE,  'sys_syn_delete',
        TRUE);

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');

DO $$BEGIN
    EXECUTE sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text,        no_diff_text,   delete_row_indicator)
VALUES (1,              'test_data1',           'not used',     FALSE);

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');

SELECT user_data.test_table_pull(FALSE);

SELECT  (in_data.id).*,
        '<Id   Attr>' AS id_attr,
        (in_data.attributes).*,
        '<Attr   NoD>' AS attr_nod,
        (in_data.no_diff).*
FROM    user_data.test_table_in_1 AS in_data;

SELECT user_data.test_table_out_move_1();

SELECT  out_queue.id,
        out_queue.delta_type,
        out_queue.queue_state,
        (in_data.id).*,
        '<Id   Attr>' AS id_attr,
        (in_data.attributes).*,
        '<Attr   NoD>' AS attr_nod,
        (in_data.no_diff).*
FROM    user_data.test_table_out_queue_1 AS out_queue
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id IN (1, 2);
UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id IN (1, 2);
SELECT user_data.test_table_out_processed_1();

SELECT  out_baseline.id,
        (in_data.id).*,
        '<Id   Attr>' AS id_attr,
        (in_data.attributes).*,
        '<Attr   NoD>' AS attr_nod,
        (in_data.no_diff).*
FROM    user_data.test_table_out_baseline_1 AS out_baseline
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id);

ROLLBACK;
