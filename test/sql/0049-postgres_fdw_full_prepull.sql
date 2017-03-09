BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE EXTENSION postgres_fdw;

DO $DOBLOCK$BEGIN
        EXECUTE $$CREATE SERVER sys_syn_contrib_regression FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host $$ ||
            quote_literal(split_part((
                    SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)) ||
            $$, dbname 'contrib_regression')$$;
END$DOBLOCK$;

CREATE USER MAPPING FOR public
    SERVER sys_syn_contrib_regression;

CREATE FOREIGN TABLE user_data.test_table_fdw (
  test_table_id integer NOT NULL,
  test_table_text text
)
SERVER  sys_syn_contrib_regression
OPTIONS (schema_name 'user_data_fdw_source', table_name 'test_table_fdw_source');

INSERT INTO sys_syn.in_groups_def VALUES ('in');

DO $$BEGIN
        EXECUTE sys_syn.prepull_create_sql('user_data.test_table_fdw'::regclass, 'in', id_columns => ARRAY['test_table_id']);
END$$;

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql(
                relation        => 'user_data.test_table_fdw_prepull_full'::regclass,
                in_group_id     => 'in',
                schema          => 'user_data',
                id_columns     => ARRAY['test_table_id'],
                no_diff_columns => NULL,
                omit_columns    => ARRAY[]::TEXT[],
                limit_to_columns=> NULL,
                full_prepull_id => 'test_table_fdw',
                changes_prepull_id=> NULL,
                in_table_id     => 'test_table_fdw');
END$$;

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table_fdw', 'out');

ALTER TABLE user_data.test_table_fdw_out_queue_1
  ADD FOREIGN KEY (trans_id_in, id) REFERENCES user_data.test_table_fdw_in_1 (trans_id_in, id) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.test_table_fdw_prepull_full();
UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 2;
SELECT user_data.test_table_fdw_pull(FALSE);
SELECT user_data.test_table_fdw_out_move_1();

SELECT id, delta_type, queue_state FROM user_data.test_table_fdw_out_queue_1;

ROLLBACK;
