BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_key integer NOT NULL,
        test_table_updated timestamp with time zone,
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
                       $COL$("test_table_text","text",Attribute,"in_source.test_table_text",,,,)$COL$
                ]::sys_syn.create_in_column[],
                'user_data.test_table',
                NULL
        );

INSERT INTO user_data.test_table(
        test_table_key, test_table_updated,             test_table_text)
VALUES  (1,              '2009-01-02 03:04:05-00',       'test_data v1'),
        (1,              '2010-01-02 03:04:05-00',       'test_data v2');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out');

ALTER TABLE user_data.test_table_out_queue
  ADD FOREIGN KEY (trans_id_in, key) REFERENCES user_data.test_table_in (trans_id_in, key) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT key, delta_type, queue_state FROM user_data.test_table_out_queue;

SELECT user_data.test_table_vacuum();

UPDATE user_data.test_table_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (key).test_table_key = 1;

SELECT user_data.test_table_out_processed();

SELECT key, delta_type, queue_state FROM user_data.test_table_out_queue;

SELECT user_data.test_table_vacuum();

ROLLBACK;
