BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION pgcrypto SCHEMA public;
CREATE EXTENSION sys_syn;

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

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out', data_view => TRUE);

ALTER TABLE user_data.test_table_out_queue_1
  ADD FOREIGN KEY (trans_id_in, id) REFERENCES user_data.test_table_in_1 (trans_id_in, id) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT * FROM user_data.test_table_out_queue_data_1;

SELECT user_data.test_table_vacuum_1();

UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;

UPDATE user_data.test_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;

SELECT user_data.test_table_out_processed_1();

SELECT * FROM user_data.test_table_out_queue_data_1;

SELECT user_data.test_table_vacuum_1();

ROLLBACK;
