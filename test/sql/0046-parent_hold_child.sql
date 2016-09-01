BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.parent_table (
        parent_table_id_1 integer NOT NULL,
        parent_table_id_2 integer NOT NULL,
        parent_table_text text,
        CONSTRAINT parent_table_pid PRIMARY KEY (parent_table_id_1, parent_table_id_2));

CREATE TABLE user_data.child_table (
        child_table_id integer NOT NULL,
        parent_table_id_1 integer,
        parent_table_id_2 integer,
        CONSTRAINT child_table_pid PRIMARY KEY (child_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_add_sql('user_data.parent_table'::regclass, 'in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql('user_data.parent_table'::regclass, 'in');
END$$;

SELECT sys_syn.in_table_add (
                schema          => 'user_data',
                in_table_id     => 'child_table',
                in_group_id     => 'in',
                in_pull_id      => NULL,
                in_columns      => ARRAY[
                       $COL$("child_table_id","integer",ID,"in_source.child_table_id",,,,)$COL$,
                       $COL$("parent_table_id_1","integer",Attribute,"in_source.parent_table_id_1",,1,"parent_table","parent_table_id_1")$COL$,
                       $COL$("parent_table_id_2","integer",Attribute,"in_source.parent_table_id_2",,1,"parent_table","parent_table_id_2")$COL$
                ]::sys_syn.create_in_column[],
                full_table_reference    => 'user_data.child_table'
        );

INSERT INTO user_data.parent_table(
        parent_table_id_1,     parent_table_id_2,     parent_table_text)
VALUES (1,                      2,                      'parent_data');

INSERT INTO user_data.child_table(
        child_table_id,        parent_table_id_1,     parent_table_id_2)
VALUES (3,                      1,                      2);

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'parent_table', 'out');

SELECT sys_syn.out_table_add('user_data', 'child_table', 'out');

ALTER TABLE user_data.parent_table_out_queue
  ADD FOREIGN KEY (trans_id_in, id) REFERENCES user_data.parent_table_in (trans_id_in, id) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.parent_table_pull(FALSE);
SELECT user_data.parent_table_out_move();
SELECT id, delta_type, queue_state FROM user_data.parent_table_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.child_table_pull(FALSE);
SELECT user_data.child_table_out_move();
SELECT id, delta_type, queue_state FROM user_data.child_table_out_queue;

UPDATE user_data.parent_table_out_queue SET queue_state = 'Claimed'::sys_syn.queue_state, hold_reason_text = 'Testing Hold' WHERE (id).parent_table_id_1 = 1;
UPDATE user_data.parent_table_out_queue SET queue_state = 'Hold'::sys_syn.queue_state, hold_reason_text = 'Testing Hold' WHERE (id).parent_table_id_1 = 1;
SELECT user_data.parent_table_out_processed();
SELECT id, delta_type, queue_state FROM user_data.parent_table_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.child_table_pull(FALSE);
SELECT user_data.child_table_out_move();
SELECT id, delta_type, queue_state FROM user_data.child_table_out_queue;

UPDATE user_data.parent_table_out_queue SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).parent_table_id_1 = 1;
UPDATE user_data.parent_table_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).parent_table_id_1 = 1;
SELECT user_data.parent_table_out_processed();
SELECT id, delta_type, queue_state FROM user_data.parent_table_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.child_table_pull(FALSE);
SELECT user_data.child_table_out_move();
SELECT id, delta_type, queue_state FROM user_data.child_table_out_queue;

ROLLBACK;
