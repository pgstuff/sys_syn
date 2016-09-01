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

SELECT sys_syn.in_table_add_sql('user_data.test_table'::regclass, 'in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES (1,              'test_data');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add(
        'user_data',
        'test_table',
        'out',
        claim_queue_count       => 1::smallint,
        queue_pid_used_age      => '1.1 hours'::INTERVAL);

ALTER TABLE user_data.test_table_out_queue
  ADD FOREIGN KEY (trans_id_in, id) REFERENCES user_data.test_table_in (trans_id_in, id) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue;

SELECT * FROM user_data.test_table_out_queue_pid_health();

SELECT user_data.test_table_out_claim(user_data.test_table_out_queue_id_claim());

SELECT * FROM user_data.test_table_out_queue_pid_health();

ROLLBACK;
