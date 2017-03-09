BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text text,
        test_table_date date,
        test_table_datetime timestamp with time zone,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');

DO $$BEGIN
    EXECUTE sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table (
        test_table_id, test_table_text,        test_table_date,        test_table_datetime)
VALUES (1,              'test_record_1',        '2010-01-02',           '2013-04-05 06:07:08-00');
INSERT INTO user_data.test_table (
        test_table_id, test_table_text,        test_table_date,        test_table_datetime)
VALUES (2,              '',                     'infinity'::DATE,       'infinity'::timestamp);
INSERT INTO user_data.test_table (
        test_table_id, test_table_text,        test_table_date,        test_table_datetime)
VALUES (3,              NULL,                   '-infinity'::DATE,      '-infinity'::timestamp);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,                priority,       data_type_like,                 in_table_id_like,       out_group_id_like,
        column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES ('time_zone_us_eastern', 50,          'timestamp with time zone',  NULL,                   NULL,
        NULL,                   NULL,                   'timestamp without time zone',
        $$%1 AT TIME ZONE 'US/Eastern'$$,
        NULL,   NULL,                   FALSE);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,                priority,       data_type_like,                 in_table_id_like,       out_group_id_like,
        column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES (NULL,                   10,             NULL,  NULL,                   NULL,
        'sys_syn_trans_id_out', NULL,                   NULL,
        NULL,
        TRUE,   NULL,                   TRUE);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        out_group_id_like,      column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES (NULL,                   101,            '{date_infinity}',      'date',                         NULL,
        NULL,                   NULL,                   NULL,                   NULL,
        $$final_id date_infinity not set!$$,
        NULL,   NULL,                   FALSE);

INSERT INTO sys_syn.out_groups_def (
        out_group_id,   out_column_transform_rule_group_ids)
VALUES ('out',          ARRAY['time_zone_us_eastern','sys_syn-mssql']);

SELECT sys_syn.out_table_create_sql('user_data', 'test_table', 'out', omit_columns => ARRAY['sys_syn_hold_reason_text'], data_view => TRUE);

DO $$BEGIN
        EXECUTE sys_syn.out_table_create_sql('user_data', 'test_table', 'out', omit_columns => ARRAY['sys_syn_hold_reason_text'], data_view => TRUE);
END$$;

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT * FROM user_data.test_table_out_queue_data_1;

UPDATE user_data.test_table_out_queue_data_1 SET sys_syn_queue_state = 'Claimed'::sys_syn.queue_state WHERE test_table_id = 1;

UPDATE user_data.test_table_out_queue_data_1 SET sys_syn_queue_state = 'Processed'::sys_syn.queue_state WHERE test_table_id = 1;

SELECT user_data.test_table_out_processed_1();

SELECT * FROM user_data.test_table_out_queue_data_1;

ROLLBACK;
