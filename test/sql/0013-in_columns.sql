BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_key integer NOT NULL,
        test_table_text varchar(255),
        test_table_date date,
        test_table_datetime timestamp with time zone,
        test_table_us_eastern_datetime timestamp without time zone,
        CONSTRAINT test_table_pkey PRIMARY KEY (test_table_key));

INSERT INTO sys_syn.in_groups_def
        (in_group_id)
VALUES  ('in');

SELECT sys_syn.in_table_add (
                'user_data',
                'test_table',
                'in',
                NULL,
                ARRAY[
                       $COL$("test_table_key","integer",Key,"in_source.test_table_key",,,,)$COL$,
                       $COL$("test_table_text","text",Attribute,"rtrim(NULLIF(in_source.test_table_text, ' '))",,,,)$COL$,
                       $COL$("test_table_date","date",Attribute,"CASE WHEN in_source.test_table_date < '1890-01-01'::DATE THEN '-infinity'::DATE WHEN in_source.test_table_date >= CURRENT_DATE + INTERVAL '25567 days' THEN 'infinity'::DATE ELSE in_source.test_table_date END",,,,)$COL$,
                       $COL$("test_table_datetime","timestamp with time zone",Attribute,"CASE WHEN in_source.test_table_datetime <= '1890-01-01 00:00:00-00'::timestamp with time zone THEN '-infinity'::timestamp with time zone WHEN in_source.test_table_datetime >= (CURRENT_DATE + INTERVAL '25567 days')::timestamp with time zone THEN 'infinity'::timestamp with time zone ELSE in_source.test_table_datetime END",,,,)$COL$,
                       $COL$("test_table_datetime2","timestamp with time zone",Attribute,"CASE WHEN in_source.test_table_us_eastern_datetime AT TIME ZONE 'US/Eastern' <= '1890-01-01 00:00:00-00'::timestamp with time zone THEN '-infinity'::timestamp with time zone WHEN in_source.test_table_us_eastern_datetime AT TIME ZONE 'US/Eastern' >= (CURRENT_DATE + INTERVAL '25567 days')::timestamp with time zone THEN 'infinity'::timestamp with time zone ELSE in_source.test_table_us_eastern_datetime AT TIME ZONE 'US/Eastern' END",,,,)$COL$
                ]::sys_syn.create_in_column[],
                'user_data.test_table',
                NULL
        );

INSERT INTO user_data.test_table (
        test_table_key, test_table_text,        test_table_date,        test_table_datetime,
        test_table_us_eastern_datetime)
VALUES (1,              'test_record_1',        '2010-01-02',           '2013-04-05 06:07:08-00',
        '2009-01-02 03:04:05');
INSERT INTO user_data.test_table (
        test_table_key, test_table_text,        test_table_date,        test_table_datetime,
        test_table_us_eastern_datetime)
VALUES (2,              ' ',                    '2099-01-01',           '2099-01-01 00:00:00-00',
        '2099-01-01 00:00:00');
INSERT INTO user_data.test_table (
        test_table_key, test_table_text,        test_table_date,        test_table_datetime,
        test_table_us_eastern_datetime)
VALUES (3,              NULL,                   '1889-12-31',           '1889-12-31 23:59:59-00',
        '1889-01-01 00:00:00');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out');

ALTER TABLE user_data.test_table_out_queue
  ADD FOREIGN KEY (trans_id_in, key) REFERENCES user_data.test_table_in (trans_id_in, key) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT  (in_data.key).*,
        COALESCE((in_data.attributes).test_table_text, '<NULL>') AS test_table_text_or_null,
        (in_data.attributes).*
FROM    user_data.test_table_out_queue out_queue
        LEFT JOIN user_data.test_table_in AS in_data USING (trans_id_in, key)
ORDER BY in_data.key;

ROLLBACK;
