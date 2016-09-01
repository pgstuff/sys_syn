CREATE SCHEMA user_data_fdw_source
    AUTHORIZATION postgres;

CREATE TABLE user_data_fdw_source.test_table_fdw_source (
        test_table_id integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO user_data_fdw_source.test_table_fdw_source(
        test_table_id, test_table_text)
VALUES (1,              'test_data');
