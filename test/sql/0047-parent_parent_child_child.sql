BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.parent_1 (
        parent_1_id integer NOT NULL,
        parent_1_text text,
        CONSTRAINT parent_1_pid PRIMARY KEY (parent_1_id));

CREATE TABLE user_data.parent_2 (
        parent_2_id integer NOT NULL,
        parent_2_text text,
        CONSTRAINT parent_2_pid PRIMARY KEY (parent_2_id));

CREATE TABLE user_data.child_1 (
        child_1_id integer NOT NULL,
        parent_1_id integer,
        CONSTRAINT child_1_pid PRIMARY KEY (child_1_id));

CREATE TABLE user_data.child_2 (
        child_2_id integer NOT NULL,
        parent_1_id integer,
        parent_2_id integer,
        CONSTRAINT child_2_pid PRIMARY KEY (child_2_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.parent_1'::regclass, 'in');
        EXECUTE sys_syn.in_table_create_sql('user_data.parent_2'::regclass, 'in');
END$$;

SELECT sys_syn.in_table_create (
                schema          => 'user_data',
                in_table_id     => 'child_1',
                in_group_id     => 'in',
                in_pull_id      => NULL,
                in_columns      => ARRAY[
                       $COL$("child_1_id","integer",Id,"in_source.child_1_id",,,,,)$COL$,
                       $COL$("parent_1_id","integer",Attribute,"in_source.parent_1_id",,1,"parent_1","parent_1_id",)$COL$
                ]::sys_syn.create_in_column[],
                full_table_reference    => 'user_data.child_1'
        );

SELECT sys_syn.in_table_create (
                schema          => 'user_data',
                in_table_id     => 'child_2',
                in_group_id     => 'in',
                in_pull_id      => NULL,
                in_columns      => ARRAY[
                       $COL$("child_2_id","integer",Id,"in_source.child_2_id",,,,,)$COL$,
                       $COL$("parent_1_id","integer",Attribute,"in_source.parent_1_id",,1,"parent_1","parent_1_id",)$COL$,
                       $COL$("parent_2_id","integer",Attribute,"in_source.parent_2_id",,2,"parent_2","parent_2_id",)$COL$
                ]::sys_syn.create_in_column[],
                full_table_reference    => 'user_data.child_2'
        );

INSERT INTO user_data.parent_1 (
        parent_1_id,   parent_1_text)
VALUES (1,              'parent_1_data');

INSERT INTO user_data.parent_2 (
        parent_2_id,   parent_2_text)
VALUES (2,              'parent_2_data');

INSERT INTO user_data.child_1 (
        child_1_id,    parent_1_id)
VALUES (3,              1);

INSERT INTO user_data.child_2 (
        child_2_id,    parent_1_id,   parent_2_id)
VALUES (4,              1,              2);

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'parent_1', 'out');

SELECT sys_syn.out_table_create('user_data', 'parent_2', 'out');

SELECT sys_syn.out_table_create('user_data', 'child_1', 'out');

SELECT sys_syn.out_table_create('user_data', 'child_2', 'out');

ALTER TABLE user_data.parent_1_out_queue_1
  ADD FOREIGN KEY (trans_id_in, id) REFERENCES user_data.parent_1_in_1 (trans_id_in, id) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.parent_1_pull(FALSE);
SELECT user_data.parent_1_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.parent_1_out_queue_1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 2;
SELECT user_data.child_1_pull(FALSE);
SELECT user_data.child_1_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.child_1_out_queue_1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 3;
SELECT user_data.child_2_pull(FALSE);
SELECT user_data.child_2_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.child_2_out_queue_1;

UPDATE user_data.parent_1_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state, hold_reason_text = 'Testing parent-child dependency.' WHERE (id).parent_1_id = 1;
UPDATE user_data.parent_1_out_queue_1 SET queue_state = 'Hold'::sys_syn.queue_state, hold_reason_text = 'Testing parent-child dependency.' WHERE (id).parent_1_id = 1;
SELECT user_data.parent_1_out_processed_1();
SELECT id, delta_type, queue_state FROM user_data.parent_1_out_queue_1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 4;
SELECT user_data.child_1_pull(FALSE);
SELECT user_data.child_1_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.child_1_out_queue_1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 5;
SELECT user_data.child_2_pull(FALSE);
SELECT user_data.child_2_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.child_2_out_queue_1;

UPDATE user_data.parent_1_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).parent_1_id = 1;
UPDATE user_data.parent_1_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).parent_1_id = 1;
SELECT user_data.parent_1_out_processed_1();
SELECT id, delta_type, queue_state FROM user_data.parent_1_out_queue_1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 6;
SELECT user_data.child_1_pull(FALSE);
SELECT user_data.child_1_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.child_1_out_queue_1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 7;
SELECT user_data.child_2_pull(FALSE);
SELECT user_data.child_2_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.child_2_out_queue_1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 8;
SELECT user_data.parent_2_pull(FALSE);
SELECT user_data.parent_2_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.parent_2_out_queue_1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 9;
SELECT user_data.child_2_pull(FALSE);
SELECT user_data.child_2_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.child_2_out_queue_1;

UPDATE user_data.parent_2_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).parent_2_id = 2;
UPDATE user_data.parent_2_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).parent_2_id = 2;
SELECT user_data.parent_2_out_processed_1();
SELECT id, delta_type, queue_state FROM user_data.parent_2_out_queue_1;

SELECT user_data.child_2_out_foreign_processed_1();
SELECT id, delta_type, queue_state FROM user_data.child_2_out_queue_1;

ROLLBACK;
