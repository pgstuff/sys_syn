BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION pgcrypto SCHEMA public;
CREATE EXTENSION sys_syn;

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,         relation_name_like,
        column_name_like,       new_data_type,  new_column_name,        new_array_order,
        expression)
VALUES (null,                   300,            '{}',                   'timestamp with time zone',null,
        '%_updated',            null,           null,                   1,
        null);

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_updated timestamp with time zone,
        test_table_text text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id, test_table_updated));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_create (
                'user_data',
                'test_table',
                'in',
                NULL,
                ARRAY[
                       $COL$("test_table_id","integer",Id,"in_source.test_table_id",,,,,)$COL$,
                       $COL$("test_table_updated","timestamp with time zone",Attribute,"in_source.test_table_updated",1,,,,)$COL$,
                       $COL$("test_table_text","text",Attribute,"in_source.test_table_text",,,,,)$COL$
                ]::sys_syn.create_in_column[],
                'user_data.test_table',
                NULL
        );

INSERT INTO user_data.test_table(
        test_table_id, test_table_updated,             test_table_text)
VALUES  (1,              '2009-01-02 03:04:05-00',       'test_data v1'),
        (1,              '2010-01-02 03:04:05-00',       'test_data v2');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');

SELECT user_data.test_table_pull(FALSE);

SELECT user_data.test_table_out_move_1();

SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue_1;

ROLLBACK;
