BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

INSERT INTO sys_syn.foreign_keys_for_c_sql (
        database_path,                                  foreign_key_id,
        foreign_in_table_id,                            primary_in_table_id,
        foreign_column_name,                            primary_column_name)
VALUES ('server_name/service_instance/database_name',   'test_fkey',
        'child_table',                                  'parent_table',
        'parent_table_id',                              'parent_table_id');

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.parent_table (
        parent_table_id integer NOT NULL,
        parent_table_text text,
        CONSTRAINT parent_table_pid PRIMARY KEY (parent_table_id));

CREATE TABLE user_data.child_table (
        child_table_id integer NOT NULL,
        parent_table_id integer,
        CONSTRAINT child_table_pid PRIMARY KEY (child_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_create_sql('user_data.parent_table'::regclass, 'in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.parent_table'::regclass, 'in');
END$$;

SELECT sys_syn.in_table_create_sql('user_data.child_table'::regclass, 'in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.child_table'::regclass, 'in');
END$$;

INSERT INTO user_data.parent_table(
        parent_table_id,       parent_table_text)
VALUES (1,                      'parent_data');

INSERT INTO user_data.child_table(
        child_table_id,        parent_table_id)
VALUES (2,                      1);

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'parent_table', 'out');

SELECT sys_syn.out_table_create('user_data', 'child_table', 'out');

SELECT user_data.parent_table_pull(FALSE);
SELECT user_data.parent_table_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.parent_table_out_queue_1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 2;
SELECT user_data.child_table_pull(FALSE);
SELECT user_data.child_table_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.child_table_out_queue_1;

UPDATE user_data.parent_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).parent_table_id = 1;
UPDATE user_data.parent_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).parent_table_id = 1;
SELECT user_data.parent_table_out_processed_1();
SELECT id, delta_type, queue_state FROM user_data.parent_table_out_queue_1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 3;
SELECT user_data.child_table_pull(FALSE);
SELECT user_data.child_table_out_move_1();
SELECT id, delta_type, queue_state FROM user_data.child_table_out_queue_1;

UPDATE user_data.child_table_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).child_table_id = 2;
UPDATE user_data.child_table_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).child_table_id = 2;
SELECT user_data.child_table_out_processed_1();
SELECT id, delta_type, queue_state FROM user_data.parent_table_out_queue_1;

ROLLBACK;
