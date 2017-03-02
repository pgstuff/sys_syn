BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION pgcrypto SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text varchar(255),
        test_table_date date,
        test_table_datetime timestamp with time zone,
        test_table_us_eastern_datetime timestamp without time zone,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,         relation_name_like,
        column_name_like,               new_data_type,                  new_column_name,
        expression)
VALUES ('convert_us_eastern',   25,             '{}',                   'timestamp without time zone',null,
        null,                           'timestamp with time zone',     null,
        $$%1 AT TIME ZONE 'US/Eastern'$$);

-- Trailing space logic should have a priority below 100.
INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,         relation_name_like,
        column_name_like,       new_data_type,
        expression)
VALUES ('space_is_null',        75,             '{}',                   'text',                 null,
        null,                   null,
        $$NULLIF(%1, ' ')$$);

-- Use infinity thresholds that are closer to commonly used values instead of the data type's min/max values.
INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,         relation_name_like,
        column_name_like,       new_data_type,
        expression)
VALUES (null,                   99,             '{date_infinity}',      'date',                 null,
        null,                   null,
        $$CASE WHEN %1 < '1890-01-01'::DATE THEN '-infinity'::DATE WHEN %1 >= CURRENT_DATE + INTERVAL '25567 days' THEN 'infinity'::DATE ELSE %1 END$$);

-- This transform will not be used because the final_id date_infinity is set by the transform above (and has the same qualifiers).
INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,         relation_name_like,
        column_name_like,       new_data_type,
        expression)
VALUES (null,                   100,            '{date_infinity}',      'date',                 null,
        null,                   null,
        $$The final_id date_infinity should prevent this transform from being used.$$);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,         relation_name_like,
        column_name_like,       new_data_type,
        expression)
VALUES (null,                   100,            '{}',                   'timestamp with time zone',null,
        null,                   null,
        $$CASE WHEN %1 <= '1890-01-01 00:00:00-00'::timestamp with time zone THEN '-infinity'::timestamp with time zone WHEN %1 >= (CURRENT_DATE + INTERVAL '25567 days')::timestamp with time zone THEN 'infinity'::timestamp with time zone ELSE %1 END$$);

-- Replace a column name.
INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,         relation_name_like,
        column_name_like,               new_data_type,          new_column_name,
        expression)
VALUES ('rename_et_dt_col_name',200,            '{}',                   'timestamp with time zone',null,
        'test_table_us_eastern_datetime',null,                  'test_table_datetime2',
        null);

INSERT INTO sys_syn.in_groups_def
        (in_group_id,   parent_in_group_id,     in_column_transform_rule_group_ids)
VALUES  ('in',          NULL,                   ARRAY['convert_us_eastern','sys_syn-general']),
        ('in2',         'in',                   NULL),
        ('in3',         'in2',                  ARRAY['space_is_null','rename_et_dt_col_name']);

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in3');

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in3');
END$$;

INSERT INTO user_data.test_table (
        test_table_id, test_table_text,        test_table_date,        test_table_datetime,
        test_table_us_eastern_datetime)
VALUES (1,              'test_record_1',        '2010-01-02',           '2013-04-05 06:07:08-00',
        '2009-01-02 03:04:05');
INSERT INTO user_data.test_table (
        test_table_id, test_table_text,        test_table_date,        test_table_datetime,
        test_table_us_eastern_datetime)
VALUES (2,              ' ',                    '2099-01-01',           '2099-01-01 00:00:00-00',
        '2099-01-01 00:00:00');
INSERT INTO user_data.test_table (
        test_table_id, test_table_text,        test_table_date,        test_table_datetime,
        test_table_us_eastern_datetime)
VALUES (3,              NULL,                   '1889-12-31',           '1889-12-31 23:59:59-00',
        '1889-01-01 00:00:00');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');

ALTER TABLE user_data.test_table_out_queue_1
  ADD FOREIGN KEY (trans_id_in, id) REFERENCES user_data.test_table_in_1 (trans_id_in, id) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT  (in_data.id).*,
        COALESCE((in_data.attributes).test_table_text, '<NULL>') AS test_table_text_or_null,
        (in_data.attributes).*
FROM    user_data.test_table_out_queue_1 out_queue
        LEFT JOIN user_data.test_table_in_1 AS in_data USING (trans_id_in, id)
ORDER BY in_data.id;

ROLLBACK;
