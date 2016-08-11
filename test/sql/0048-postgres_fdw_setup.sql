CREATE SCHEMA user_data_fdw_source
    AUTHORIZATION postgres;

CREATE TABLE user_data_fdw_source.test_table_fdw_source (
        test_table_key integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pkey PRIMARY KEY (test_table_key));

INSERT INTO user_data_fdw_source.test_table_fdw_source(
        test_table_key, test_table_text)
VALUES (1,              'test_data');
