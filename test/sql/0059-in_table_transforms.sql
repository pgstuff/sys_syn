BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
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

INSERT INTO sys_syn.in_table_transforms(
        rule_group_id,          priority,       final_ids,              in_group_id_like,       in_table_id_like,
        new_in_partition_count)
VALUES ('in_partition_count',   50,             '{}',                   '%',                    null,
        4);

INSERT INTO sys_syn.in_groups_def
        (in_group_id,   parent_in_group_id,     rule_group_ids)
VALUES  ('in',          NULL,                   ARRAY['in_partition_count']),
        ('in2',         'in',                   NULL),
        ('in3',         'in2',                  ARRAY['in_partition_count']);

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in3');

INSERT INTO sys_syn.in_table_transforms(
        rule_group_id,          priority,       final_ids,              in_group_id_like,       schema_like,    in_table_id_like,
        omit)
VALUES (null,                   20,             '{}',                   null,                   'user_%',       null,
        true);

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in', 'user_data');

ROLLBACK;
