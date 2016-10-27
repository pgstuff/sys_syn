BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_create (
                'user_data',
                'test_table',
                'in',
                NULL,
                ARRAY[
                       $COL$("test_table_id","integer",Id,"in_source.test_table_id",,,,)$COL$,
                       $COL$("test_table_text","text",Attribute,"in_source.test_table_text",,,,)$COL$
                ]::sys_syn.create_in_column[],
                'user_data.test_table',
                NULL
        );

ROLLBACK;
