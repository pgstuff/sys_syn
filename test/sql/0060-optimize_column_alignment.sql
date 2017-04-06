BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_id_int integer NOT NULL,
        test_id_bigint bigint NOT NULL,
        test_id_text text NOT NULL,
        test_attr_text text,
        test_attr_int integer,
        test_array_int integer NOT NULL,
        test_attr_bigint bigint,
        test_attr_date date,
        test_attr_int8range int8range,
        test_attr_timestamp timestamp with time zone,
        CONSTRAINT test_table_pid PRIMARY KEY (test_id_int, test_id_bigint, test_id_text, test_array_int));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       in_table_id_like,       column_name_like,       new_in_column_type,     new_array_order)
VALUES (null,                   200,            'test_table',           'test_array_int',       'Attribute',            1);

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in', optimize_column_alignment => true);

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in', optimize_column_alignment => true, order_by_in_column_type => true);

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in', optimize_column_alignment => true, order_by_in_column_type => true, order_by_attribute_array => true);

ROLLBACK;
