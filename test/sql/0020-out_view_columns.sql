BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');

DO $$BEGIN
    EXECUTE sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES (1,              'test_data');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out',
                out_columns => ARRAY[
                       $COL$("test_table_id","(in_source.id).test_table_id",,,Id)$COL$,
                       $COL$("processing_state","CASE WHEN out_queue.queue_state = 'Unclaimed'::sys_syn.queue_state THEN 1 WHEN out_queue.queue_state = 'Claimed'::sys_syn.queue_state THEN 2 WHEN out_queue.queue_state = 'Processed'::sys_syn.queue_state THEN 3 WHEN out_queue.queue_state = 'Hold'::sys_syn.queue_state THEN 4 ELSE NULL END","queue_state","CASE WHEN new.processing_state = 1 THEN 'Unclaimed'::sys_syn.queue_state WHEN new.processing_state = 2 THEN 'Claimed'::sys_syn.queue_state WHEN new.processing_state = 3 THEN 'Processed'::sys_syn.queue_state WHEN new.processing_state = 4 THEN 'Hold'::sys_syn.queue_state END",Attribute)$COL$,
                       $COL$("test_table_text_upper","UPPER((in_source.attributes).test_table_text)",,,Attribute)$COL$
                ]::sys_syn.create_out_column[],
                data_view => TRUE);

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT * FROM user_data.test_table_out_queue_data_1;

UPDATE user_data.test_table_out_queue_data_1 SET processing_state = 2 WHERE test_table_id = 1;

UPDATE user_data.test_table_out_queue_data_1 SET processing_state = 3 WHERE test_table_id = 1;

SELECT user_data.test_table_out_processed_1();

SELECT * FROM user_data.test_table_out_queue_data_1;

ROLLBACK;
