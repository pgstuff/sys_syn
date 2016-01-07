BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_key integer NOT NULL,
        test_table_updated timestamp with time zone,
        test_table_date date,
        test_table_text text,
        CONSTRAINT test_table_pkey PRIMARY KEY (test_table_key, test_table_updated));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_add (
                'user_data',
                'test_table',
                'in',
                NULL,
                ARRAY[
                       $COL$("test_table_key","integer",Key,"in_source.test_table_key",,,,)$COL$,
                       $COL$("test_table_updated","timestamp with time zone",Attribute,"in_source.test_table_updated",1,,,)$COL$,
                       $COL$("test_table_date","date",Attribute,"in_source.test_table_date",,,,)$COL$,
                       $COL$("test_table_text","text",Attribute,"in_source.test_table_text",,,,)$COL$
                ]::sys_syn.create_in_column[],
                'user_data.test_table',
                NULL
        );

INSERT INTO user_data.test_table(
        test_table_key, test_table_updated,             test_table_date,        test_table_text)
VALUES  (1,             '2009-01-02 03:04:05-00',       '2009-02-01',           'test_data v1'),
        (1,             '2010-01-02 03:04:05-00',       '2009-03-01',           'test_data v2'),
        (2,             '2011-01-02 03:04:05-00',       '2009-04-01',           'test_data');

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
VALUES ('upper',                150,            'text',                         NULL,                   'out',
        NULL,                   NULL,                   NULL,
        $$UPPER(%1) || ' - ' || %1$$,
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

INSERT INTO sys_syn.out_groups_def (
        out_group_id,   out_column_transform_rule_group_ids)
VALUES ('out',          ARRAY['time_zone_us_eastern','sys_syn-mssql','upper']);

SELECT sys_syn.out_table_add_sql('user_data', 'test_table', 'out', omit_columns => ARRAY['sys_syn_hold_reason_text'], data_view => TRUE);

DO $$BEGIN
    EXECUTE sys_syn.out_table_add_sql('user_data', 'test_table', 'out', omit_columns => ARRAY['sys_syn_hold_reason_text'], data_view => TRUE);
END$$;

ALTER TABLE user_data.test_table_out_queue
  ADD FOREIGN KEY (trans_id_in, key) REFERENCES user_data.test_table_in (trans_id_in, key) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT * FROM user_data.test_table_out_queue_data;

SELECT user_data.test_table_vacuum();

UPDATE user_data.test_table_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (key).test_table_key = 1;

SELECT user_data.test_table_out_processed();

SELECT * FROM user_data.test_table_out_queue_data;

SELECT user_data.test_table_vacuum();

ROLLBACK;
