BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.parent_1 (
        parent_1_key integer NOT NULL,
        parent_1_text text,
        CONSTRAINT parent_1_pkey PRIMARY KEY (parent_1_key));

CREATE TABLE user_data.parent_2 (
        parent_2_key integer NOT NULL,
        parent_2_text text,
        CONSTRAINT parent_2_pkey PRIMARY KEY (parent_2_key));

CREATE TABLE user_data.child_1 (
        child_1_key integer NOT NULL,
        parent_1_key integer,
        CONSTRAINT child_1_pkey PRIMARY KEY (child_1_key));

CREATE TABLE user_data.child_2 (
        child_2_key integer NOT NULL,
        parent_1_key integer,
        parent_2_key integer,
        CONSTRAINT child_2_pkey PRIMARY KEY (child_2_key));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql('user_data.parent_1'::regclass, 'in');
        EXECUTE sys_syn.in_table_add_sql('user_data.parent_2'::regclass, 'in');
END$$;

SELECT sys_syn.in_table_add (
                schema          => 'user_data',
                in_table_id     => 'child_1',
                in_group_id     => 'in',
                in_pull_id      => NULL,
                in_columns      => ARRAY[
                       $COL$("child_1_key","integer",Key,"in_source.child_1_key",,,,)$COL$,
                       $COL$("parent_1_key","integer",Attribute,"in_source.parent_1_key",,1,"parent_1","parent_1_key")$COL$
                ]::sys_syn.create_in_column[],
                full_table_reference    => 'user_data.child_1'
        );

SELECT sys_syn.in_table_add (
                schema          => 'user_data',
                in_table_id     => 'child_2',
                in_group_id     => 'in',
                in_pull_id      => NULL,
                in_columns      => ARRAY[
                       $COL$("child_2_key","integer",Key,"in_source.child_2_key",,,,)$COL$,
                       $COL$("parent_1_key","integer",Attribute,"in_source.parent_1_key",,1,"parent_1","parent_1_key")$COL$,
                       $COL$("parent_2_key","integer",Attribute,"in_source.parent_2_key",,2,"parent_2","parent_2_key")$COL$
                ]::sys_syn.create_in_column[],
                full_table_reference    => 'user_data.child_2'
        );

INSERT INTO user_data.parent_1 (
        parent_1_key,   parent_1_text)
VALUES (1,              'parent_1_data');

INSERT INTO user_data.parent_2 (
        parent_2_key,   parent_2_text)
VALUES (2,              'parent_2_data');

INSERT INTO user_data.child_1 (
        child_1_key,    parent_1_key)
VALUES (3,              1);

INSERT INTO user_data.child_2 (
        child_2_key,    parent_1_key,   parent_2_key)
VALUES (4,              1,              2);

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'parent_1', 'out');

SELECT sys_syn.out_table_add('user_data', 'parent_2', 'out');

SELECT sys_syn.out_table_add('user_data', 'child_1', 'out');

SELECT sys_syn.out_table_add('user_data', 'child_2', 'out');

ALTER TABLE user_data.parent_1_out_queue
  ADD FOREIGN KEY (trans_id_in, key) REFERENCES user_data.parent_1_in (trans_id_in, key) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.parent_1_pull(FALSE);
SELECT user_data.parent_1_out_move();
SELECT key, delta_type, queue_state FROM user_data.parent_1_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.child_1_pull(FALSE);
SELECT user_data.child_1_out_move();
SELECT key, delta_type, queue_state FROM user_data.child_1_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.child_2_pull(FALSE);
SELECT user_data.child_2_out_move();
SELECT key, delta_type, queue_state FROM user_data.child_2_out_queue;

UPDATE user_data.parent_1_out_queue SET queue_state = 'Hold'::sys_syn.queue_state, hold_reason_text = 'Testing parent-child dependency.' WHERE (key).parent_1_key = 1;
SELECT user_data.parent_1_out_processed();
SELECT key, delta_type, queue_state FROM user_data.parent_1_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.child_1_pull(FALSE);
SELECT user_data.child_1_out_move();
SELECT key, delta_type, queue_state FROM user_data.child_1_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.child_2_pull(FALSE);
SELECT user_data.child_2_out_move();
SELECT key, delta_type, queue_state FROM user_data.child_2_out_queue;

UPDATE user_data.parent_1_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (key).parent_1_key = 1;
SELECT user_data.parent_1_out_processed();
SELECT key, delta_type, queue_state FROM user_data.parent_1_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.child_1_pull(FALSE);
SELECT user_data.child_1_out_move();
SELECT key, delta_type, queue_state FROM user_data.child_1_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.child_2_pull(FALSE);
SELECT user_data.child_2_out_move();
SELECT key, delta_type, queue_state FROM user_data.child_2_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.parent_2_pull(FALSE);
SELECT user_data.parent_2_out_move();
SELECT key, delta_type, queue_state FROM user_data.parent_2_out_queue;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;
SELECT user_data.child_2_pull(FALSE);
SELECT user_data.child_2_out_move();
SELECT key, delta_type, queue_state FROM user_data.child_2_out_queue;

UPDATE user_data.parent_2_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (key).parent_2_key = 2;
SELECT user_data.parent_2_out_processed();
SELECT key, delta_type, queue_state FROM user_data.parent_2_out_queue;

SELECT user_data.child_2_out_foreign_processed();
SELECT key, delta_type, queue_state FROM user_data.child_2_out_queue;

ROLLBACK;
