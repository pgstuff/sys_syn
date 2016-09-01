BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_updated timestamp with time zone,
        test_table_text text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id, test_table_updated));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

DO $$BEGIN
        EXECUTE sys_syn.pre_pull_add_sql('user_data.test_table'::regclass, 'in');
END$$;

SELECT sys_syn.in_table_add (
                schema          => 'user_data'::regnamespace,
                in_table_id     => 'test_table',
                in_group_id     => 'in',
                in_pull_id      => NULL,
                in_columns      => ARRAY[
                       $COL$("trans_id_in","sys_syn.trans_id",TransIdIn,"in_source.trans_id_in",,,,)$COL$,
                       $COL$("test_table_id","integer",ID,"in_source.test_table_id",,,,)$COL$,
                       $COL$("test_table_updated","timestamp with time zone",Attribute,"in_source.test_table_updated",1,,,)$COL$,
                       $COL$("test_table_text","text",Attribute,"in_source.test_table_text",,,,)$COL$
                ]::sys_syn.create_in_column[],
                full_table_reference    => 'user_data.test_table_prepull_full',
                changes_table_reference => NULL,
                enable_deletes_implied  => TRUE,
                full_prepull_id         => 'test_table',
                changes_prepull_id      => NULL
        );

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out');

ALTER TABLE user_data.test_table_out_queue
  ADD FOREIGN KEY (trans_id_in, id) REFERENCES user_data.test_table_in (trans_id_in, id) ON UPDATE RESTRICT ON DELETE RESTRICT;

INSERT INTO user_data.test_table(
        test_table_id, test_table_updated,             test_table_text)
VALUES  (1,             '2009-01-02 03:04:05-00',       'test_data v1'),
        (1,             '2010-01-02 03:04:05-00',       'test_data v2'),
        (2,             '2011-01-02 03:04:05-00',       'test_data');

SELECT user_data.test_table_prepull_full();
UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;

SELECT user_data.test_table_pull(FALSE);

SELECT user_data.test_table_out_move();

SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue;

UPDATE user_data.test_table_out_queue SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;

UPDATE user_data.test_table_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;

SELECT user_data.test_table_out_processed();

SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue;

ROLLBACK;
