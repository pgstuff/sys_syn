BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_key integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pkey PRIMARY KEY (test_table_key));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_add_sql('user_data.test_table'::regclass, 'in');

DO $$BEGIN
    EXECUTE sys_syn.in_table_add_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table(
        test_table_key, test_table_text)
VALUES (1,              'test_data');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out',
                out_columns => ARRAY[
                       $COL$("test_table_key","(in_source.key).test_table_key",,)$COL$,
                       $COL$("processing_state","CASE WHEN out_queue.queue_state = 'Unread'::sys_syn.queue_state THEN 1 WHEN out_queue.queue_state = 'Reading'::sys_syn.queue_state THEN 2 WHEN out_queue.queue_state = 'Processed'::sys_syn.queue_state THEN 3 WHEN out_queue.queue_state = 'Hold'::sys_syn.queue_state THEN 4 ELSE NULL END","queue_state","CASE WHEN new.processing_state = 1 THEN 'Unread'::sys_syn.queue_state WHEN new.processing_state = 2 THEN 'Reading'::sys_syn.queue_state WHEN new.processing_state = 3 THEN 'Processed'::sys_syn.queue_state WHEN new.processing_state = 4 THEN 'Hold'::sys_syn.queue_state END")$COL$,
                       $COL$("test_table_text_upper","UPPER((in_source.attributes).test_table_text)",,)$COL$
                ]::sys_syn.create_out_column[],
                data_view => TRUE);

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT * FROM user_data.test_table_out_queue_data;

UPDATE user_data.test_table_out_queue_data SET processing_state = 3 WHERE test_table_key = 1;

SELECT user_data.test_table_out_processed();

SELECT * FROM user_data.test_table_out_queue_data;

ROLLBACK;
