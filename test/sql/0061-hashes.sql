BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION citext;
CREATE EXTENSION sys_syn;

CREATE TYPE single_column_test AS (column_name text);

CREATE TABLE test_hash_values (
        test_order integer,
        text_to_hash single_column_test
);

INSERT INTO test_hash_values VALUES (1, '()'::single_column_test);
INSERT INTO test_hash_values VALUES (2, '(123456789)'::single_column_test);
INSERT INTO test_hash_values VALUES (3, '(1234567890)'::single_column_test);
INSERT INTO test_hash_values VALUES (4, '(The quick brown fox jumps over the lazy dog)'::single_column_test);
INSERT INTO test_hash_values VALUES (5, '(test)'::single_column_test);
INSERT INTO test_hash_values VALUES (6, '(TEST)'::single_column_test);
INSERT INTO test_hash_values VALUES (7, '(a)'::single_column_test);
INSERT INTO test_hash_values VALUES (8, '(abc)'::single_column_test);

SELECT  (text_to_hash).column_name,
        to_hex(sys_syn.hash_id(text_to_hash))      AS hash_hex,    sys_syn.hash_id(text_to_hash)  AS hash_int,
        to_hex(sys_syn.crc32_id(text_to_hash))     AS crc32_hex,   sys_syn.crc32_id(text_to_hash) AS crc32_int,
        to_hex(sys_syn.crc32c_id(text_to_hash))    AS crc32c_hex,  sys_syn.crc32c_id(text_to_hash)AS crc32c_int
FROM    test_hash_values
ORDER BY test_order;

CREATE TYPE columns_test AS (column_text_1 text, column_text_2 text, column_int integer, column_smallint smallint, column_bigint bigint);

CREATE TABLE test_hash_values_2 (
        test_order integer,
        columns_to_hash columns_test
);

INSERT INTO test_hash_values_2 VALUES (1, '(,,,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (2, '(12345,6789,,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (3, '(The quick brown fox, jumps over the lazy dog,,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (4, '(te,st,,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (5, '(TE,ST,,,)'::columns_test);

SELECT  (columns_to_hash).column_text_1,
        (columns_to_hash).column_text_2,
        to_hex(sys_syn.hash_id(columns_to_hash))   AS hash_hex,    sys_syn.hash_id(columns_to_hash)   AS hash_int,
        to_hex(sys_syn.crc32_id(columns_to_hash))  AS crc32_hex,   sys_syn.crc32_id(columns_to_hash)  AS crc32_int,
        to_hex(sys_syn.crc32c_id(columns_to_hash)) AS crc32c_hex,  sys_syn.crc32c_id(columns_to_hash) AS crc32c_int
FROM    test_hash_values_2
ORDER BY test_order;

TRUNCATE test_hash_values_2;

-- .column_int
INSERT INTO test_hash_values_2 VALUES ( 6, '(,,0,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES ( 7, '(,,1,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES ( 8, '(,,-2147483648,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES ( 9, '(,,2147483647,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (10, '(,,-32768,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (11, '(,,32767,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (12, '(,,-126,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (13, '(,,127,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (14, '(,,-1234567898,,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (15, '(,,1234567898,,)'::columns_test);
-- column_smallint
INSERT INTO test_hash_values_2 VALUES (16, '(,,,0,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (17, '(,,,1,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (18, '(,,,-32768,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (19, '(,,,32767,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (20, '(,,,-126,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (21, '(,,,127,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (22, '(,,,-12345,)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (23, '(,,,12345,)'::columns_test);
-- column_bigint
INSERT INTO test_hash_values_2 VALUES (24, '(,,,,0)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (25, '(,,,,1)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (26, '(,,,,-9223372036854775808)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (27, '(,,,,9223372036854775807)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (28, '(,,,,-2147483648)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (29, '(,,,,2147483647)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (30, '(,,,,-32768)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (31, '(,,,,32767)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (32, '(,,,,-126)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (33, '(,,,,127)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (34, '(,,,,-1234567899876543210)'::columns_test);
INSERT INTO test_hash_values_2 VALUES (35, '(,,,,1234567899876543210)'::columns_test);

SELECT  (columns_to_hash).column_int,
        (columns_to_hash).column_smallint,
        (columns_to_hash).column_bigint,
        to_hex(sys_syn.hash_id(columns_to_hash))        AS hash_hex,    sys_syn.hash_id(columns_to_hash)        AS hash_int,
        to_hex(sys_syn.crc32_id(columns_to_hash))       AS crc32_hex,   sys_syn.crc32_id(columns_to_hash)       AS crc32_int,
        to_hex(sys_syn.crc32c_id(columns_to_hash))      AS crc32c_hex,  sys_syn.crc32c_id(columns_to_hash)      AS crc32c_int
FROM    test_hash_values_2
ORDER BY COALESCE(
                (columns_to_hash).column_int,
                (columns_to_hash).column_smallint,
                (columns_to_hash).column_bigint),
        test_order;

CREATE TYPE datatype_test AS (
        column_decimal decimal,
        column_real real,
        column_double double precision,
        column_money money,
        column_bytea bytea,
        column_timestamp timestamp without time zone,
        column_timestamptz timestamp with time zone,
        column_date date,
        column_time time without time zone,
        column_timetz time with time zone,
        column_interval interval,
        column_boolean boolean,
        column_point point,
        column_line line,
        column_uuid uuid,
        column_int4range int4range,
        column_daterange daterange);

CREATE TABLE test_hash_values_3 (
        test_order integer,
        columns_to_hash datatype_test
);

INSERT INTO test_hash_values_3 VALUES ( 1, E'(1234.56,,,,,,,,,,,,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES ( 2, E'(,1234.56,,,,,,,,,,,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES ( 3, E'(,,1234.56,,,,,,,,,,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES ( 4, E'(,,,"$1234.56",,,,,,,,,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES ( 5, E'(,,,,\\\\xDEADBEEF,,,,,,,,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES ( 6, E'(,,,,,"2017-12-31 12:34:56.123456",,,,,,,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES ( 7, E'(,,,,,,"2017-12-31 12:34:56.123456-05",,,,,,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES ( 8, E'(,,,,,,,2017-12-31,,,,,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES ( 9, E'(,,,,,,,,"12:34:56.123456",,,,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES (10, E'(,,,,,,,,,"12:34:56.123456-05",,,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES (11, E'(,,,,,,,,,,"1-2",,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES (12, E'(,,,,,,,,,,,true,,,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES (14, E'(,,,,,,,,,,,,"(1.2,3.4)",,,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES (15, E'(,,,,,,,,,,,,,"{1.2,3.4,5.6}",,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES (16, E'(,,,,,,,,,,,,,,a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,,)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES (17, E'(,,,,,,,,,,,,,,,"[1,2)",)'::datatype_test);
INSERT INTO test_hash_values_3 VALUES (18, E'(,,,,,,,,,,,,,,,,"[2017-01-01,2017-12-31)")'::datatype_test);
INSERT INTO test_hash_values_3 VALUES (19, E'(1234.56,1234.56,1234.56,"$1234.56",\\\\xDEADBEEF,"2017-12-31 12:34:56.123456","2017-12-31 12:34:56.123456-05",2017-12-31,"12:34:56.123456","12:34:56.123456-05","1-2",true,"(1.2,3.4)","{1.2,3.4,5.6}",a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,"[1,2)","[2017-01-01,2017-12-31)")'::datatype_test);

SELECT  (columns_to_hash).*,
        to_hex(sys_syn.hash_id(columns_to_hash))        AS hash_hex,    sys_syn.hash_id(columns_to_hash)        AS hash_int,
        to_hex(sys_syn.crc32_id(columns_to_hash))       AS crc32_hex,   sys_syn.crc32_id(columns_to_hash)       AS crc32_int,
        to_hex(sys_syn.crc32c_id(columns_to_hash))      AS crc32c_hex,  sys_syn.crc32c_id(columns_to_hash)      AS crc32c_int
FROM    test_hash_values_3
ORDER BY test_order;

CREATE TYPE custom_datatypes AS (
        column_tinyint tinyint,
        column_citext citext);

CREATE TABLE test_hash_values_4 (
        test_order integer,
        columns_to_hash custom_datatypes
);

INSERT INTO test_hash_values_4 VALUES ( 1, '(0,)'::custom_datatypes);
INSERT INTO test_hash_values_4 VALUES ( 2, '(127,)'::custom_datatypes);
INSERT INTO test_hash_values_4 VALUES ( 3, '(-126,)'::custom_datatypes);
INSERT INTO test_hash_values_4 VALUES ( 4, '(1,)'::custom_datatypes);
INSERT INTO test_hash_values_4 VALUES ( 5, '(123,)'::custom_datatypes);

INSERT INTO test_hash_values_4 VALUES ( 6, '(,test)'::custom_datatypes);
INSERT INTO test_hash_values_4 VALUES ( 7, '(,TEST)'::custom_datatypes);

SELECT  (columns_to_hash).*,
        to_hex(sys_syn.hash_id(columns_to_hash))        AS hash_hex,    sys_syn.hash_id(columns_to_hash)        AS hash_int,
        to_hex(sys_syn.crc32_id(columns_to_hash))       AS crc32_hex,   sys_syn.crc32_id(columns_to_hash)       AS crc32_int,
        to_hex(sys_syn.crc32c_id(columns_to_hash))      AS crc32c_hex,  sys_syn.crc32c_id(columns_to_hash)      AS crc32c_int
FROM    test_hash_values_4
ORDER BY test_order;

ROLLBACK;
