/*
 * Author: The maintainer's name
 * Created at: Tue Nov 24 16:47:02 -0500 2015
 *
 */

--
-- This is a example code genereted automaticaly
-- by pgxn-utils.

SET client_min_messages = warning;

CREATE SCHEMA sys_syn;
ALTER SCHEMA sys_syn OWNER TO postgres;

DO $$BEGIN
        IF EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname = 'varint') THEN
                CREATE DOMAIN sys_syn.trans_id AS public.varuint64;
        ELSE
                CREATE DOMAIN sys_syn.trans_id AS bigint;
        END IF;
        ALTER DOMAIN sys_syn.trans_id OWNER TO postgres;
END$$;

CREATE TYPE sys_syn.in_column_type AS ENUM (
        'Key',
        'Attribute',
        'NoDiff',
        'TransIdIn'
);
ALTER TYPE sys_syn.in_column_type OWNER TO postgres;

CREATE TYPE sys_syn.create_in_column AS (
        column_name             text,
        data_type               text,
        in_column_type          sys_syn.in_column_type,
        source_in_expression    text,
        array_order             smallint,
        foreign_key_index       smallint,
        primary_in_table_id     text,
        primary_column_name     text
);
ALTER TYPE sys_syn.create_in_column OWNER TO postgres;

CREATE TYPE sys_syn.delta_type AS ENUM (
        'Add',
        'Change',
        'Delete'
);
ALTER TYPE sys_syn.delta_type OWNER TO postgres;

CREATE TYPE sys_syn.queue_state AS ENUM (
        'Unread',
        'Reading',
        'Processed',
        'Hold'
);
ALTER TYPE sys_syn.queue_state OWNER TO postgres;

-- Queue columns that can be directly updated by the queue consumer.
CREATE TYPE sys_syn.queue_column AS ENUM (
        'queue_state',
        'queue_id',
        'queue_priority',
        'hold_reason_id',
        'hold_reason_text',
        'processed_time'
);
ALTER TYPE sys_syn.queue_column OWNER TO postgres;

CREATE TYPE sys_syn.create_out_column AS (
        column_name text,
        column_expression text,
        queue_column_name sys_syn.queue_column,
        queue_column_expression text
);
ALTER TYPE sys_syn.create_out_column OWNER TO postgres;


CREATE TABLE sys_syn.settings (
        logical_replication boolean NOT NULL DEFAULT false
);
ALTER TABLE sys_syn.settings OWNER TO postgres;
CREATE UNIQUE INDEX settings_1_row_idx
        ON sys_syn.settings
        USING btree
        ((true));

CREATE TABLE sys_syn.prepulls_def (
        prepull_id text NOT NULL,
        lock_id serial NOT NULL,
        schema regnamespace NOT NULL
);
ALTER TABLE sys_syn.prepulls_def OWNER TO postgres;
ALTER TABLE ONLY sys_syn.prepulls_def
        ADD CONSTRAINT prepulls_def_pkey PRIMARY KEY (prepull_id);

CREATE TABLE sys_syn.in_groups_def (
        in_group_id text NOT NULL,
        parent_in_group_id text,
        in_column_transform_rule_group_ids text[] DEFAULT NULL::text[]
);
ALTER TABLE sys_syn.in_groups_def OWNER TO postgres;
ALTER TABLE ONLY sys_syn.in_groups_def
        ADD CONSTRAINT in_groups_def_pkey PRIMARY KEY (in_group_id);
ALTER TABLE sys_syn.in_groups_def
        ADD CONSTRAINT in_groups_def_parent_in_group_id_fkey FOREIGN KEY (parent_in_group_id)
                REFERENCES sys_syn.in_groups_def (in_group_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE TABLE sys_syn.in_column_transforms (
        rule_group_id           text,
        priority                smallint NOT NULL,
        data_type_like          text,
        relation_name_like      text,
        in_column_type          sys_syn.in_column_type,
        column_name_like        text,
        new_data_type           text,
        new_in_column_type      sys_syn.in_column_type,
        new_column_name         text,
        expression              text,
        create_in_columns       sys_syn.create_in_column[],
        omit                    boolean,
        final_ids               text[] DEFAULT '{}'::text[] NOT NULL,
        final_rule              boolean DEFAULT FALSE NOT NULL
);
ALTER TABLE sys_syn.in_column_transforms OWNER TO postgres;
ALTER TABLE sys_syn.in_column_transforms
        ADD CONSTRAINT priority_disallow_sign CHECK (priority >= 0);
CREATE UNIQUE INDEX ON sys_syn.in_column_transforms (
        priority, data_type_like, relation_name_like, in_column_type, column_name_like);

CREATE TABLE sys_syn.in_pulls_def (
        in_pull_id      text NOT NULL,
        schema          regnamespace NOT NULL,
        lock_id         integer NOT NULL,
        pull_pre_sql    text,
        pull_post_sql   text
);
ALTER TABLE sys_syn.in_pulls_def OWNER TO postgres;
CREATE SEQUENCE sys_syn.in_pulls_def_lock_id_seq
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
ALTER TABLE sys_syn.in_pulls_def_lock_id_seq OWNER TO postgres;
ALTER SEQUENCE sys_syn.in_pulls_def_lock_id_seq OWNED BY sys_syn.in_pulls_def.lock_id;
ALTER TABLE ONLY sys_syn.in_pulls_def ALTER COLUMN lock_id SET DEFAULT nextval('sys_syn.in_pulls_def_lock_id_seq'::regclass);
ALTER TABLE ONLY sys_syn.in_pulls_def
        ADD CONSTRAINT in_pulls_def_pkey PRIMARY KEY (in_pull_id);
ALTER TABLE sys_syn.in_pulls_def
        ADD CONSTRAINT lock_id_disallow_sign CHECK (lock_id >= 0);

CREATE OR REPLACE FUNCTION sys_syn.util_column_name_to_in_column_type(in_table_id text, column_name name)
        RETURNS sys_syn.in_column_type
    LANGUAGE plpgsql STABLE
    AS $_$
BEGIN
        RETURN NULL;
END;
$_$;
ALTER FUNCTION sys_syn.util_column_name_to_in_column_type(in_table_id text, column_name name) OWNER TO postgres;

CREATE TABLE sys_syn.in_tables_def (
        in_table_id             text            NOT NULL,
        in_group_id             text            NOT NULL,
        in_pull_id              text            NOT NULL,
        in_pull_order           smallint        NOT NULL,
        schema                  regnamespace    NOT NULL,
        attributes_array        boolean         NOT NULL,
        full_prepull_id         text,
        changes_prepull_id      text,
        full_table_reference    text,
        changes_table_reference text,
        full_sql                text,
        changes_sql             text,
        full_pre_sql            text,
        changes_pre_sql         text,
        full_post_sql           text,
        changes_post_sql        text,
        enable_deletes_implied  boolean DEFAULT TRUE NOT NULL,
        record_comparison_different     text,
        record_comparison_same          text
);
ALTER TABLE sys_syn.in_tables_def OWNER TO postgres;
ALTER TABLE ONLY sys_syn.in_tables_def
        ADD CONSTRAINT in_tables_def_pkey PRIMARY KEY (in_table_id);
ALTER TABLE ONLY sys_syn.in_tables_def
        ADD CONSTRAINT in_tables_def_in_group_id_fkey FOREIGN KEY (in_group_id) REFERENCES sys_syn.in_groups_def(in_group_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY sys_syn.in_tables_def
        ADD CONSTRAINT in_tables_def_in_pull_id_fkey FOREIGN KEY (in_pull_id) REFERENCES sys_syn.in_pulls_def(in_pull_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY sys_syn.in_tables_def
        ADD CONSTRAINT in_tables_def_full_prepull_id_fkey FOREIGN KEY (full_prepull_id) REFERENCES sys_syn.prepulls_def(prepull_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY sys_syn.in_tables_def
        ADD CONSTRAINT in_tables_def_changes_prepull_id_fkey
                FOREIGN KEY (changes_prepull_id) REFERENCES sys_syn.prepulls_def(prepull_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE sys_syn.in_tables_def
        ADD CONSTRAINT in_pull_order_disallow_sign CHECK (in_pull_order >= 0);

CREATE TABLE sys_syn.in_table_columns_def (
        in_table_id             text            NOT NULL,
        column_index            smallint        NOT NULL,
        array_order             smallint,
        column_name             name            NOT NULL,
        source_in_expression    text
);
ALTER TABLE sys_syn.in_table_columns_def OWNER TO postgres;
ALTER TABLE ONLY sys_syn.in_table_columns_def
        ADD CONSTRAINT in_table_columns_def_pkey PRIMARY KEY (in_table_id, column_name);
ALTER TABLE sys_syn.in_table_columns_def
  ADD CONSTRAINT in_table_columns_def_in_table_id_fkey FOREIGN KEY (in_table_id)
      REFERENCES sys_syn.in_tables_def (in_table_id) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;
/* TODO:  pg_dump has issues with this.
ALTER TABLE ONLY sys_syn.in_table_columns_def
        ADD CONSTRAINT in_table_columns_def_column_exists
                CHECK (sys_syn.util_column_name_to_in_column_type(in_table_id, column_name) IS NOT NULL);*/
ALTER TABLE sys_syn.in_table_columns_def
        ADD CONSTRAINT column_index_disallow_sign CHECK (column_index >= 0);
ALTER TABLE sys_syn.in_table_columns_def
        ADD CONSTRAINT array_order_disallow_sign CHECK (array_order >= 0);

CREATE TABLE sys_syn.in_foreign_keys (
        primary_table_id        text NOT NULL,
        foreign_table_id        text NOT NULL,
        foreign_key_index       smallint NOT NULL,
        primary_column_name     text NOT NULL,
        foreign_column_name     text NOT NULL
);
ALTER TABLE sys_syn.in_foreign_keys OWNER TO postgres;
ALTER TABLE ONLY sys_syn.in_foreign_keys
        ADD CONSTRAINT in_foreign_keys_pkey
                PRIMARY KEY (primary_table_id, foreign_table_id, foreign_key_index, primary_column_name, foreign_column_name);
ALTER TABLE sys_syn.in_foreign_keys
        ADD CONSTRAINT in_foreign_keys_primary_fkey FOREIGN KEY (primary_table_id, primary_column_name)
                REFERENCES sys_syn.in_table_columns_def (in_table_id, column_name) MATCH SIMPLE
                ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE sys_syn.in_foreign_keys
        ADD CONSTRAINT in_foreign_keys_foreign_fkey FOREIGN KEY (foreign_table_id, foreign_column_name)
                REFERENCES sys_syn.in_table_columns_def (in_table_id, column_name) MATCH SIMPLE
                ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE TABLE sys_syn.in_pulls_request (
        in_pull_request_id      integer NOT NULL,
        in_pull_id              text NOT NULL,
        request_full            boolean NOT NULL
);
ALTER TABLE sys_syn.in_pulls_request OWNER TO postgres;
ALTER TABLE ONLY sys_syn.in_pulls_request
        ADD CONSTRAINT in_pulls_request_pkey PRIMARY KEY (in_pull_request_id);
ALTER TABLE ONLY sys_syn.in_pulls_request
        ADD CONSTRAINT in_pulls_request_in_pull_id_fkey FOREIGN KEY (in_pull_id) REFERENCES sys_syn.in_pulls_def(in_pull_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE SEQUENCE sys_syn.in_pulls_request_in_pull_request_id_seq
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
ALTER TABLE sys_syn.in_pulls_request_in_pull_request_id_seq OWNER TO postgres;
ALTER SEQUENCE sys_syn.in_pulls_request_in_pull_request_id_seq OWNED BY sys_syn.in_pulls_request.in_pull_request_id;
ALTER TABLE ONLY sys_syn.in_pulls_request ALTER COLUMN in_pull_request_id
        SET DEFAULT nextval('sys_syn.in_pulls_request_in_pull_request_id_seq'::regclass);

CREATE TABLE sys_syn.in_pulls_state (
        in_pull_id              text NOT NULL,
        last_pull_full_start    timestamp with time zone,
        last_pull_full_finish   timestamp with time zone,
        last_pull_start         timestamp with time zone,
        last_pull_finish        timestamp with time zone
);
ALTER TABLE sys_syn.in_pulls_state OWNER TO postgres;
ALTER TABLE ONLY sys_syn.in_pulls_state
        ADD CONSTRAINT in_pulls_state_pkey PRIMARY KEY (in_pull_id);
ALTER TABLE ONLY sys_syn.in_pulls_state
        ADD CONSTRAINT in_pulls_state_in_pull_id_fkey FOREIGN KEY (in_pull_id) REFERENCES sys_syn.in_pulls_def(in_pull_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE TABLE sys_syn.in_trans_log (
        trans_id_in     sys_syn.trans_id NOT NULL,
        pre_pull        boolean,
        changes_only    boolean,
        in_table_ids    text[] NOT NULL,
        trans_time      timestamp with time zone DEFAULT now() NOT NULL,
        finish_time     timestamp with time zone
);
ALTER TABLE sys_syn.in_trans_log OWNER TO postgres;
ALTER TABLE ONLY sys_syn.in_trans_log
        ADD CONSTRAINT in_trans_log_pkey PRIMARY KEY (trans_id_in);

CREATE TABLE sys_syn.out_groups_def (
        out_group_id                            text NOT NULL,
        parent_out_group_id                     text,
        out_column_transform_rule_group_ids     text[]
);
ALTER TABLE sys_syn.out_groups_def OWNER TO postgres;
ALTER TABLE ONLY sys_syn.out_groups_def
        ADD CONSTRAINT out_groups_def_pkey PRIMARY KEY (out_group_id);
ALTER TABLE sys_syn.out_groups_def
        ADD CONSTRAINT out_groups_def_parent_fkey FOREIGN KEY (parent_out_group_id)
                REFERENCES sys_syn.out_groups_def (out_group_id) MATCH SIMPLE
                ON UPDATE RESTRICT ON DELETE NO ACTION;

CREATE TABLE sys_syn.out_tables_def (
        in_table_id             text            NOT NULL,
        out_group_id            text            NOT NULL,
        lock_id                 integer         NOT NULL,
        schema                  regnamespace    NOT NULL,
        data_view               boolean         NOT NULL,
        out_log_lifetime        interval,
        notification_channel    text,
        enable_adds             boolean DEFAULT TRUE NOT NULL,
        enable_changes          boolean DEFAULT TRUE NOT NULL,
        enable_deletes          boolean DEFAULT TRUE NOT NULL,
        condition_sql           text,
        claim_limit_rows        integer DEFAULT 2147483647 NOT NULL,
        claim_queue_count       smallint,
        claim_fixed_by_key      boolean DEFAULT false NOT NULL,
        claim_random_sample     smallint,
        queue_pid_used_age      interval,
        record_comparison_different     text,
        record_comparison_same          text
);
ALTER TABLE sys_syn.out_tables_def OWNER TO postgres;
ALTER TABLE ONLY sys_syn.out_tables_def
        ADD CONSTRAINT out_tables_def_pkey PRIMARY KEY (in_table_id, out_group_id);
ALTER TABLE ONLY sys_syn.out_tables_def
        ADD CONSTRAINT out_tables_def_in_table_id_fkey FOREIGN KEY (in_table_id) REFERENCES sys_syn.in_tables_def(in_table_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY sys_syn.out_tables_def
        ADD CONSTRAINT out_tables_def_out_group_id_fkey FOREIGN KEY (out_group_id) REFERENCES sys_syn.out_groups_def(out_group_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;
CREATE SEQUENCE sys_syn.out_tables_def_lock_id_seq
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
ALTER TABLE sys_syn.out_tables_def_lock_id_seq OWNER TO postgres;
ALTER SEQUENCE sys_syn.out_tables_def_lock_id_seq OWNED BY sys_syn.out_tables_def.lock_id;
ALTER TABLE ONLY sys_syn.out_tables_def ALTER COLUMN lock_id SET DEFAULT nextval('sys_syn.out_tables_def_lock_id_seq'::regclass);
ALTER TABLE sys_syn.out_tables_def
        ADD CONSTRAINT lock_id_disallow_sign CHECK (lock_id >= 0);

CREATE TABLE sys_syn.out_tables_state (
        in_table_id             text NOT NULL,
        out_group_id            text NOT NULL,
        trans_id_in_latest      sys_syn.trans_id DEFAULT 0
);
ALTER TABLE sys_syn.out_tables_state OWNER TO postgres;
ALTER TABLE ONLY sys_syn.out_tables_state
        ADD CONSTRAINT out_tables_state_pkey PRIMARY KEY (in_table_id, out_group_id);
ALTER TABLE ONLY sys_syn.out_tables_state
        ADD CONSTRAINT out_tables_state_out_table_id_fkey FOREIGN KEY (in_table_id, out_group_id)
                REFERENCES sys_syn.out_tables_def(in_table_id, out_group_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE TABLE sys_syn.trans_id_mod (
        trans_id_mod sys_syn.trans_id NOT NULL
);
ALTER TABLE sys_syn.trans_id_mod OWNER TO postgres;

CREATE TABLE sys_syn.out_view_columns_def (
        in_table_id             text            NOT NULL,
        out_group_id            text            NOT NULL,
        column_index            smallint        NOT NULL,
        column_name             text            NOT NULL,
        column_expression       text            NOT NULL,
        queue_column_name       sys_syn.queue_column,
        queue_column_expression text
);
ALTER TABLE sys_syn.out_view_columns_def OWNER TO postgres;
ALTER TABLE ONLY sys_syn.out_view_columns_def
        ADD CONSTRAINT out_view_columns_def_pkey PRIMARY KEY (in_table_id, out_group_id, column_name);
ALTER TABLE ONLY sys_syn.out_view_columns_def
        ADD CONSTRAINT out_view_columns_def_in_table_id_fkey FOREIGN KEY (in_table_id, out_group_id)
                REFERENCES sys_syn.out_tables_def (in_table_id, out_group_id) MATCH SIMPLE ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE sys_syn.out_view_columns_def
        ADD CONSTRAINT column_index_disallow_sign CHECK (column_index >= 0);

CREATE TABLE sys_syn.out_column_transforms (
        rule_group_id           text,
        priority                smallint NOT NULL,
        data_type_like          text,
        in_table_id_like        text,
        out_group_id_like       text,
        schema_like             text,
        in_column_type          sys_syn.in_column_type,
        column_name_like        text,
        new_data_type           text,
        new_column_name         text,
        expression              text,
        create_out_columns      sys_syn.create_out_column[],
        omit                    boolean,
        final_ids               text[] DEFAULT '{}'::text[] NOT NULL,
        final_rule              boolean DEFAULT FALSE NOT NULL
);
ALTER TABLE sys_syn.out_column_transforms OWNER TO postgres;
ALTER TABLE sys_syn.out_column_transforms
        ADD CONSTRAINT priority_disallow_sign CHECK (priority >= 0);
CREATE UNIQUE INDEX ON sys_syn.out_column_transforms (
        priority, data_type_like, in_table_id_like, out_group_id_like, schema_like, in_column_type, column_name_like);

CREATE TABLE sys_syn.in_pull_sequences_def (
        in_pull_sequence_id text NOT NULL
);
ALTER TABLE sys_syn.in_pull_sequences_def OWNER TO postgres;
ALTER TABLE ONLY sys_syn.in_pull_sequences_def
        ADD CONSTRAINT in_pull_sequences_def_pkey PRIMARY KEY (in_pull_sequence_id);

CREATE TABLE sys_syn.in_pull_sequence_pulls (
        in_pull_sequence_id     text            NOT NULL,
        sequence_index          smallint        NOT NULL,
        in_pull_id              text            NOT NULL
);
ALTER TABLE sys_syn.in_pull_sequence_pulls OWNER TO postgres;
ALTER TABLE ONLY sys_syn.in_pull_sequence_pulls
        ADD CONSTRAINT in_pull_sequence_pulls_pkey PRIMARY KEY (in_pull_sequence_id, in_pull_id);
ALTER TABLE ONLY sys_syn.in_pull_sequence_pulls
        ADD CONSTRAINT in_pull_sequences_def_in_pull_sequence_id_fkey FOREIGN KEY (in_pull_sequence_id)
                REFERENCES sys_syn.in_pull_sequences_def (in_pull_sequence_id) MATCH SIMPLE ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY sys_syn.in_pull_sequence_pulls
        ADD CONSTRAINT in_pull_sequences_def_in_pull_id_fkey FOREIGN KEY (in_pull_id)
                REFERENCES sys_syn.in_pulls_def (in_pull_id) MATCH SIMPLE ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE sys_syn.in_pull_sequence_pulls
        ADD CONSTRAINT sequence_index_disallow_sign CHECK (sequence_index >= 0);

-- Transforms priority:
--  50 Change data type due to database technology
-- 100 Normalize minimum and maximum data value range due to database technology & remove trailing spaces.
-- Use a lower priority and set the final_id to override select transforms.

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,      data_type_like, relation_name_like,     column_name_like,
        new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-general',      50,             '{}',           'character(%)', null,                   null,
        'text',                 null,
        null);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,      data_type_like, relation_name_like,     column_name_like,
        new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-general',      50,             '{}',           'character varying',null,               null,
        'text',                 null,
        null);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,      data_type_like, relation_name_like,     column_name_like,
        new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-general',      50,             '{}',           'character varying(%)',null,            null,
        'text',                 null,
        null);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,      data_type_like, relation_name_like,     column_name_like,
        new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-general',      100,            '{text_trim}',  'text',         null,                   null,
        null,                   null,
        'rtrim(%1)');

/*INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,      data_type_like, relation_name_like,     column_name_like,
        new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-general',      50,             '{}',           'integer',      null,                   null,
        'public.varint64',      null,
        null);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,      data_type_like, relation_name_like,     column_name_like,
        new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-general',      50,             '{}',           'bigint',       null,                   null,
        'public.varint64',      null,
        null);*/

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like, relation_name_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-mariadb',      100,            '{date_infinity}',      'date',         null,
        null,                   null,                   null,
     $$CASE WHEN %1 <= '1000-01-01'::DATE THEN '-infinity'::DATE WHEN %1 >= '9999-12-31'::DATE THEN 'infinity'::DATE ELSE %1 END$$);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 relation_name_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-mariadb',      100,            '{timestamp_infinity}', 'timestamp without time zone',  null,
        null,                   null,                   null,
        $$CASE WHEN %1 <= '1000-01-01 00:00:00'::timestamp without time zone $$ ||
                $$THEN '-infinity'::timestamp without time zone $$ ||
                $$WHEN %1 >= '9999-12-31 23:59:59'::timestamp without time zone $$ ||
                $$THEN 'infinity'::timestamp without time zone ELSE %1 END$$);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 relation_name_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-oracle',       100,            '{date_infinity}',      'date',                         null,
        null,                   null,                   null,
  $$CASE WHEN %1 <= '4712-01-01 BC'::DATE THEN '-infinity'::DATE WHEN %1 >= '9999-12-31'::DATE THEN 'infinity'::DATE ELSE %1 END$$);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 relation_name_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-oracle',       100,            '{timestamp_infinity}',  'timestamp without time zone',  null,
        null,                   null,                   null,
        $$CASE WHEN %1 <= '4712-01-01 00:00:00 BC'::timestamp without time zone $$ ||
                $$THEN '-infinity'::timestamp without time zone $$ ||
                $$WHEN %1 >= '9999-12-31 23:59:59'::timestamp without time zone $$ ||
                $$THEN 'infinity'::timestamp without time zone ELSE %1 END$$);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 relation_name_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-oracle',       100,            '{timestamptz_infinity}','timestamp with time zone',     null,
        null,                   null,                   null,
        $$CASE WHEN %1 <= '4712-01-01 00:00:00-00 BC'::timestamp with time zone $$ ||
                $$THEN '-infinity'::timestamp with time zone $$ ||
                $$WHEN %1 >= '9999-12-31 23:59:59-00'::timestamp with time zone $$ ||
                $$THEN 'infinity'::timestamp with time zone ELSE %1 END$$);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 relation_name_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-mssql',        100,            '{date_infinity}',      'date',                         null,
        null,                   null,                   null,
     $$CASE WHEN %1 <= '0001-01-01'::DATE THEN '-infinity'::DATE WHEN %1 >= '9999-12-31'::DATE THEN 'infinity'::DATE ELSE %1 END$$);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 relation_name_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-mssql',        100,            '{smalldatetime_infinity}','timestamp without time zone', null,
        '%smalldatetime',       null,                   null,
        $$CASE WHEN %1 <= '1900-01-01 00:00:00'::timestamp without time zone $$ ||
                $$THEN '-infinity'::timestamp without time zone $$ ||
                $$WHEN %1 >= '2079-06-06 23:59:59'::timestamp without time zone $$ ||
                $$THEN 'infinity'::timestamp without time zone ELSE %1 END$$);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 relation_name_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-mssql',        100,            '{datetime_infinity}',  'timestamp without time zone',  null,
        '%datetime',            null,                   null,
        $$CASE WHEN %1 <= '1753-01-01 00:00:00'::timestamp without time zone $$ ||
                $$THEN '-infinity'::timestamp without time zone $$ ||
                $$WHEN %1 >= '9999-12-31 23:59:59'::timestamp without time zone $$ ||
                $$THEN 'infinity'::timestamp without time zone ELSE %1 END$$);

INSERT INTO sys_syn.in_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 relation_name_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES ('sys_syn-mssql',        100,            '{datetime2_infinity}', 'timestamp without time zone',  null,
        '%datetime2',           null,                   null,
        $$CASE WHEN %1 <= '0001-01-01 00:00:00'::timestamp without time zone $$ ||
                $$THEN '-infinity'::timestamp without time zone $$ ||
                $$WHEN %1 >= '9999-12-31 23:59:59'::timestamp without time zone $$ ||
                $$THEN 'infinity'::timestamp without time zone ELSE %1 END$$);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        out_group_id_like,      column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES ('sys_syn-mariadb',      100,            '{date_infinity}',      'date',                         NULL,
        NULL,                   NULL,                   NULL,                   NULL,
        $$CASE WHEN %1 < '1000-01-01'::DATE THEN '1000-01-01'::DATE WHEN %1 > '9999-12-31'::DATE THEN '9999-12-31'::DATE $$ ||
                $$ELSE %1 END$$,
        NULL,   NULL,                   FALSE);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        out_group_id_like,      column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES ('sys_syn-mariadb',      100,            '{timestamp_infinity}', 'timestamp without time zone',  NULL,
        NULL,                   NULL,                   NULL,                   NULL,
        $$CASE WHEN %1 < '1000-01-01 00:00:00'::timestamp without time zone $$ ||
                $$THEN '1000-01-01 00:00:00'::timestamp without time zone $$ ||
                $$WHEN %1 > '9999-12-31 23:59:59.999999'::timestamp without time zone $$ ||
                $$THEN '9999-12-31 23:59:59.999999'::timestamp without time zone ELSE %1 END$$,
        NULL,   NULL,                   FALSE);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        out_group_id_like,      column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES ('sys_syn-oracle',        100,           '{date_infinity}',      'date',                         NULL,
        NULL,                   NULL,                   NULL,                   NULL,
        $$CASE WHEN %1 < '4712-01-01 BC'::DATE THEN '4712-01-01 BC'::DATE WHEN %1 > '9999-12-31'::DATE $$ ||
                $$THEN '9999-12-31'::DATE ELSE %1 END$$,
        NULL,   NULL,                   FALSE);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        out_group_id_like,      column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES ('sys_syn-oracle',       100,            '{timestamp_infinity}', 'timestamp without time zone',  NULL,
        NULL,                   NULL,                   NULL,                   NULL,
        $$CASE WHEN %1 < '4712-01-01 00:00:00 BC'::timestamp without time zone $$ ||
                $$THEN '4712-01-01 00:00:00 BC'::timestamp without time zone $$ ||
                $$WHEN %1 > '9999-12-31 23:59:59.999999'::timestamp without time zone $$ ||
                $$THEN '9999-12-31 23:59:59.999999'::timestamp without time zone ELSE %1 END$$,
        NULL,   NULL,                   FALSE);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        out_group_id_like,      column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES ('sys_syn-oracle',       100,            '{timestamptz_infinity}','timestamp with time zone',     NULL,
        NULL,                   NULL,                   NULL,                   NULL,
        $$CASE WHEN %1 < '4712-01-01 00:00:00-00 BC'::timestamp with time zone $$ ||
                $$THEN '4712-01-01 00:00:00-00 BC'::timestamp with time zone $$ ||
                $$WHEN %1 > '9999-12-31 23:59:59.999999-00'::timestamp with time zone $$ ||
                $$THEN '9999-12-31 23:59:59.999999-00'::timestamp with time zone ELSE %1 END$$,
        NULL,   NULL,                   FALSE);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        out_group_id_like,      column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES ('sys_syn-mssql',        100,            '{date_infinity}',      'date',                         NULL,
        NULL,                   NULL,                   NULL,                   NULL,
        $$CASE WHEN %1 < '0001-01-01'::DATE THEN '0001-01-01'::DATE WHEN %1 > '9999-12-31'::DATE THEN '9999-12-31'::DATE $$ ||
                $$ELSE %1 END$$,
        NULL,   NULL,                   FALSE);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        out_group_id_like,      column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES ('sys_syn-mssql',        100,            '{smalldatetime_infinity}','timestamp without time zone', NULL,
        NULL,                   '%smalldatetime',       NULL,                   NULL,
        $$CASE WHEN %1 < '1900-01-01 00:00:00'::timestamp without time zone $$ ||
                $$THEN '1900-01-01 00:00:00'::timestamp without time zone $$ ||
                $$WHEN %1 > '2079-06-06 23:59:59'::timestamp without time zone $$ ||
                $$THEN '2079-06-06 23:59:59'::timestamp without time zone ELSE date_trunc('second', %1) END$$,
        NULL,   NULL,                   FALSE);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        out_group_id_like,      column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES ('sys_syn-mssql',        100,            '{datetime_infinity}',  'timestamp without time zone',  NULL,
        NULL,                   '%datetime',            NULL,                   NULL,
        $$CASE WHEN %1 < '1753-01-01 00:00:00'::timestamp without time zone $$ ||
                $$THEN '1753-01-01 00:00:00'::timestamp without time zone $$ ||
                $$WHEN %1 > '9999-12-31 23:59:59.997'::timestamp without time zone $$ ||
                $$THEN '9999-12-31 23:59:59.997'::timestamp without time zone ELSE date_trunc('milliseconds', %1) END$$,
        NULL,   NULL,                   FALSE);

INSERT INTO sys_syn.out_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        out_group_id_like,      column_name_like,       in_column_type,         new_data_type,
        expression,
        omit,   new_column_name,        final_rule)
VALUES ('sys_syn-mssql',        100,            '{datetime2_infinity}', 'timestamp without time zone',  NULL,
        NULL,                   '%datetime2',           NULL,                   NULL,
        $$CASE WHEN %1 < '0001-01-01 00:00:00'::timestamp without time zone $$ ||
                $$THEN '0001-01-01 00:00:00'::timestamp without time zone $$ ||
                $$WHEN %1 > '9999-12-31 23:59:59.9999999'::timestamp without time zone $$ ||
                $$THEN '9999-12-31 23:59:59.9999999'::timestamp without time zone ELSE %1 END$$,
        NULL,   NULL,                   FALSE);

CREATE OR REPLACE FUNCTION sys_syn.in_column_transforms_check_new() RETURNS TRIGGER AS $$
BEGIN
        IF NEW.rule_group_id LIKE 'sys_syn%' THEN
                RAISE EXCEPTION 'Rules starting with sys_syn are reserved for the sys_syn extension.'
                USING HINT = 'Choose a rule_group_id that does not begin with sys_syn.';
        END IF;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
ALTER FUNCTION sys_syn.in_column_transforms_check_new() OWNER TO postgres;

CREATE OR REPLACE FUNCTION sys_syn.in_column_transforms_check_old() RETURNS TRIGGER AS $$
BEGIN
        IF OLD.rule_group_id LIKE 'sys_syn%' THEN
                RAISE EXCEPTION 'Rules starting with sys_syn are reserved for the sys_syn extension.'
                USING HINT = 'Choose a rule_group_id that does not begin with sys_syn.';
        END IF;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
ALTER FUNCTION sys_syn.in_column_transforms_check_old() OWNER TO postgres;

CREATE CONSTRAINT TRIGGER in_column_transforms_check_new
        AFTER INSERT OR UPDATE ON sys_syn.in_column_transforms
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE PROCEDURE sys_syn.in_column_transforms_check_new();

CREATE CONSTRAINT TRIGGER in_column_transforms_check_old
        AFTER DELETE ON sys_syn.in_column_transforms
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE PROCEDURE sys_syn.in_column_transforms_check_old();

CREATE OR REPLACE FUNCTION sys_syn.out_column_transforms_check_new() RETURNS TRIGGER AS $$
BEGIN
        IF NEW.rule_group_id LIKE 'sys_syn%' THEN
                RAISE EXCEPTION 'Rules starting with sys_syn are reserved for the sys_syn extension.'
                USING HINT = 'Choose a rule_group_id that does not begin with sys_syn.';
        END IF;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
ALTER FUNCTION sys_syn.out_column_transforms_check_new() OWNER TO postgres;

CREATE OR REPLACE FUNCTION sys_syn.out_column_transforms_check_old() RETURNS TRIGGER AS $$
BEGIN
        IF OLD.rule_group_id LIKE 'sys_syn%' THEN
                RAISE EXCEPTION 'Rules starting with sys_syn are reserved for the sys_syn extension.'
                USING HINT = 'Choose a rule_group_id that does not begin with sys_syn.';
        END IF;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
ALTER FUNCTION sys_syn.out_column_transforms_check_old() OWNER TO postgres;

CREATE CONSTRAINT TRIGGER out_column_transforms_check_new
        AFTER INSERT OR UPDATE ON sys_syn.out_column_transforms
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE PROCEDURE sys_syn.out_column_transforms_check_new();

CREATE CONSTRAINT TRIGGER out_column_transforms_check_old
        AFTER DELETE ON sys_syn.out_column_transforms
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE PROCEDURE sys_syn.out_column_transforms_check_old();


CREATE FUNCTION sys_syn.distribute_load(range_seconds int) RETURNS void
        LANGUAGE plpgsql
        COST 30000
        AS $_$
BEGIN
        PERFORM pg_sleep(range_seconds / 3 + floor(random() * range_seconds / 3));
END;
$_$;
ALTER FUNCTION sys_syn.distribute_load(range_seconds int) OWNER TO postgres;

CREATE FUNCTION sys_syn.node_id_local_get()
        RETURNS text
        LANGUAGE plpgsql IMMUTABLE
        COST 20
        AS
$BODY$
DECLARE
        _node_id text;
BEGIN
        IF NOT (SELECT settings.logical_replication FROM sys_syn.settings) THEN
                RETURN '';
        END IF;

        IF EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname = 'bdr') THEN
                EXECUTE $$SELECT bdr.bdr_get_local_node_id()$$
                INTO  _node_id;

                RETURN _node_id;
        ELSIF EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname = 'pglogical') THEN
                EXECUTE $$SELECT node_id::TEXT FROM pglogical.local_node$$
                INTO  _node_id;

                RETURN _node_id;
        END IF;

        RETURN '';
END;
$BODY$;
ALTER FUNCTION sys_syn.node_id_local_get() OWNER TO postgres;

CREATE FUNCTION sys_syn.util_table_create_run_state()
        RETURNS text
        LANGUAGE plpgsql IMMUTABLE
        COST 10
        AS
$BODY$
BEGIN
        IF (SELECT settings.logical_replication FROM sys_syn.settings) THEN
                RETURN 'CREATE TABLE ';
        END IF;

        RETURN 'CREATE UNLOGGED TABLE ';
END;
$BODY$;
ALTER FUNCTION sys_syn.util_table_create_run_state() OWNER TO postgres;

CREATE FUNCTION sys_syn.in_table_add(
        schema                  regnamespace,
        in_table_id             text,
        in_group_id             text,
        in_pull_id              text,
        in_columns              sys_syn.create_in_column[],
        full_table_reference    text DEFAULT NULL,
        changes_table_reference text DEFAULT NULL,
        full_sql                text DEFAULT NULL,
        changes_sql             text DEFAULT NULL,
        full_pre_sql            text DEFAULT NULL,
        changes_pre_sql         text DEFAULT NULL,
        full_post_sql           text DEFAULT NULL,
        changes_post_sql        text DEFAULT NULL,
        enable_deletes_implied  boolean DEFAULT TRUE,
        full_prepull_id         text DEFAULT NULL,
        changes_prepull_id      text DEFAULT NULL,
        record_comparison_different     text DEFAULT NULL,
        record_comparison_same          text DEFAULT NULL)
        RETURNS void
        LANGUAGE plpgsql
        COST 10
        AS $_$
DECLARE
        _in_pull_id             TEXT;
        _attributes_array       BOOLEAN;
        _sql_buffer             TEXT;
        _sql_delimit            BOOLEAN;
        _in_column              sys_syn.create_in_column;
        _column_index           SMALLINT;
        _foreign_key_id         TEXT;
        _foreign_key_index      SMALLINT;
        _primary_column_type    sys_syn.in_column_type;
BEGIN
        IF NOT EXISTS (SELECT FROM sys_syn.settings) THEN
                INSERT INTO sys_syn.settings SELECT;

                IF EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname IN ('pglogical', 'bdr')) THEN
                        UPDATE  sys_syn.settings
                        SET     logical_replication = TRUE;
                END IF;
        END IF;

        IF in_pull_id IS NULL THEN
                _in_pull_id := in_table_id;

                INSERT INTO sys_syn.in_pulls_def(
                        in_pull_id,     schema)
                VALUES (_in_pull_id,    schema);
        ELSE
                _in_pull_id := in_pull_id;
        END IF;

        _attributes_array := FALSE;
        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        LOOP
                IF _in_column.array_order IS NOT NULL THEN
                        _attributes_array := TRUE;
                END IF;
        END LOOP;

        INSERT INTO sys_syn.in_tables_def (
                schema,         in_table_id,    in_group_id,    in_pull_id,     attributes_array,
                in_pull_order,
                full_prepull_id,                changes_prepull_id,
                full_table_reference,           changes_table_reference,
                full_sql,                       changes_sql,
                full_pre_sql,                   changes_pre_sql,
                full_post_sql,                  changes_post_sql,
                enable_deletes_implied, record_comparison_different,    record_comparison_same)
        VALUES (
                schema,         in_table_id,    in_group_id,    _in_pull_id,    _attributes_array,
                (SELECT COALESCE(MAX(in_pull_order), 0) + 1 FROM sys_syn.in_tables_def
                 WHERE in_tables_def.in_pull_id = in_table_add.in_group_id),
                full_prepull_id,                changes_prepull_id,
                full_table_reference,           changes_table_reference,
                full_sql,                       changes_sql,
                full_pre_sql,                   changes_pre_sql,
                full_post_sql,                  changes_post_sql,
                enable_deletes_implied, record_comparison_different,    record_comparison_same);

        _sql_buffer := 'CREATE TYPE ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in_key') || ' AS (';
        _sql_delimit := FALSE;
        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        WHERE   in_column_rel.in_column_type = 'Key'::sys_syn.in_column_type
        LOOP
                IF _sql_delimit THEN
                        _sql_buffer := _sql_buffer || ',';
                ELSE
                        _sql_delimit = TRUE;
                END IF;

                _sql_buffer := _sql_buffer || '
        '||quote_ident(_in_column.column_name)||'       '||_in_column.data_type||'';
        END LOOP;
        IF _sql_delimit = FALSE THEN
                RAISE EXCEPTION '1 or more key columns are required.'
                        USING HINT = 'Find the primary key columns for this table and switch the in_column_type to Key.';
        END IF;
        _sql_buffer := _sql_buffer || ');
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE TYPE ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in_attributes') || ' AS (';
        _sql_delimit := FALSE;
        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        WHERE   in_column_rel.in_column_type = 'Attribute'::sys_syn.in_column_type
        LOOP
                IF _sql_delimit THEN
                        _sql_buffer := _sql_buffer || ',';
                ELSE
                        _sql_delimit = TRUE;
                END IF;

                _sql_buffer := _sql_buffer || '
        '||quote_ident(_in_column.column_name)||'       '||_in_column.data_type||'';
        END LOOP;
        _sql_buffer := _sql_buffer || ');
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE TYPE ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in_no_diff') || ' AS (';
        _sql_delimit := FALSE;
        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        WHERE   in_column_rel.in_column_type = 'NoDiff'::sys_syn.in_column_type
        LOOP
                IF _sql_delimit THEN
                        _sql_buffer := _sql_buffer || ',';
                ELSE
                        _sql_delimit = TRUE;
                END IF;

                _sql_buffer := _sql_buffer || '
        '||quote_ident(_in_column.column_name)||'       '||_in_column.data_type||'';
        END LOOP;
        _sql_buffer := _sql_buffer || ');
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _column_index := 0;
        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        LOOP
                _column_index := _column_index + 1;

                INSERT INTO sys_syn.in_table_columns_def (
                        in_table_id,    column_index,   array_order,            column_name,
                        source_in_expression)
                VALUES (
                        in_table_id,    _column_index,  _in_column.array_order, _in_column.column_name,
                        _in_column.source_in_expression);
        END LOOP;

        _sql_buffer := 'CREATE TABLE ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in') || ' (
        trans_id_in sys_syn.trans_id DEFAULT sys_syn.trans_id_get() NOT NULL,
        key ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in_key') || ' NOT NULL,
        attributes ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in_attributes') ||
        CASE WHEN _attributes_array THEN '[]' ELSE '' END || ',
        no_diff ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in_no_diff') || '
);';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'ALTER TABLE ONLY ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in') || '
        ADD CONSTRAINT ' || quote_ident(in_table_id||'_in_pkey') || ' PRIMARY KEY (trans_id_in, key);';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'ALTER TABLE ONLY ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in') || '
        ADD CONSTRAINT ' || quote_ident(in_table_id||'_in_sys_syn_trans_id_fkey') || ' FOREIGN KEY (trans_id_in)
        REFERENCES sys_syn.in_trans_log(trans_id_in) ON UPDATE RESTRICT ON DELETE RESTRICT;';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE TABLE ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_exclude') || ' (
        key ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in_key') || ' NOT NULL
);';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'ALTER TABLE ONLY ' || quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_exclude') || '
        ADD CONSTRAINT ' || quote_ident(in_table_id||'_exclude_pkey') || ' PRIMARY KEY (key);';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _foreign_key_id := '';
        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        WHERE   in_column_rel.foreign_key_index         IS NOT NULL OR
                in_column_rel.primary_in_table_id       IS NOT NULL OR
                in_column_rel.primary_column_name       IS NOT NULL
        ORDER BY
                in_column_rel.foreign_key_index,
                in_column_rel.primary_in_table_id
        LOOP
                IF _in_column.primary_in_table_id IS NULL OR _in_column.primary_column_name IS NULL THEN

                END IF;

                IF _foreign_key_id != (_in_column.foreign_key_index || '-' || _in_column.primary_in_table_id) THEN
                        _foreign_key_index := (
                                SELECT  COALESCE(MAX(in_foreign_keys.foreign_key_index), 0) + 1
                                FROM    sys_syn.in_foreign_keys
                                WHERE   in_foreign_keys.primary_table_id = _in_column.primary_in_table_id AND
                                        in_foreign_keys.foreign_table_id = in_table_id);

                        _foreign_key_id = _in_column.foreign_key_index || '-' || _in_column.primary_in_table_id;
                END IF;

                _primary_column_type := sys_syn.util_column_name_to_in_column_type(
                        _in_column.primary_in_table_id, _in_column.primary_column_name);

                INSERT INTO sys_syn.in_foreign_keys(
                        primary_table_id,               foreign_table_id,               foreign_key_index,
                        primary_column_name,            foreign_column_name)
                VALUES (
                        _in_column.primary_in_table_id, in_table_id,                    _foreign_key_index,
                        _in_column.primary_column_name, _in_column.column_name);
        END LOOP;

        PERFORM sys_syn.util_in_table_code (
                (SELECT in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = in_table_add.in_table_id)
        );

        PERFORM sys_syn.util_in_pulls_code (
                (SELECT in_pulls_def
                FROM    sys_syn.in_pulls_def
                WHERE   in_pulls_def.in_pull_id = _in_pull_id)
        );
END;
$_$;
ALTER FUNCTION sys_syn.in_table_add(
        schema                  regnamespace,
        in_table_id             text,
        in_group_id             text,
        in_pull_id              text,
        in_columns              sys_syn.create_in_column[],
        full_table_reference    text,
        changes_table_reference text,
        full_sql                text,
        changes_sql             text,
        full_pre_sql            text,
        changes_pre_sql         text,
        full_post_sql           text,
        changes_post_sql        text,
        enable_deletes_implied  boolean,
        full_prepull_id         text,
        changes_prepull_id      text,
        record_comparison_different     text,
        record_comparison_same          text) OWNER TO postgres;

CREATE FUNCTION sys_syn.in_table_add_sql(
        relation                        regclass,
        in_group_id                     text,
        schema                          regnamespace DEFAULT NULL::regnamespace,
        key_columns                     name[] DEFAULT NULL::name[],
        no_diff_columns                 name[] DEFAULT NULL::name[],
        omit_columns                    name[] DEFAULT NULL::name[],
        limit_to_columns                name[] DEFAULT NULL::name[],
        full_prepull_id                 text DEFAULT NULL::text,
        changes_prepull_id              text DEFAULT NULL::text,
        in_table_id                     text DEFAULT NULL::text
)
        RETURNS text
        LANGUAGE plpgsql COST 10
        AS $_$
DECLARE
        _in_column_transform_rule_group_ids text[];
        _sql_buffer             TEXT;
        _column                 pg_catalog.pg_attribute%ROWTYPE;
        _column_name            TEXT;
        _in_column_type         sys_syn.in_column_type;
        _primary_key            int2vector;
        _source_in_expression   TEXT;
        _format_type            TEXT;
        _in_column_transform    sys_syn.in_column_transforms%ROWTYPE;
        _create_in_columns      sys_syn.create_in_column[];
        _create_in_column       sys_syn.create_in_column;
        _final_ids              text[];
        _omit                   boolean;
        _last_priority          smallint;
BEGIN
        _sql_buffer := NULL;

        _in_column_transform_rule_group_ids := (
                WITH RECURSIVE all_transform_rule_group_ids(parent_in_group_id, in_column_transform_rule_group_ids) AS (
                        SELECT  in_groups_def.parent_in_group_id,
                                in_groups_def.in_column_transform_rule_group_ids
                        FROM    sys_syn.in_groups_def
                        WHERE   in_groups_def.in_group_id = in_table_add_sql.in_group_id
                        UNION ALL
                        SELECT  in_groups_def.parent_in_group_id,
                                in_groups_def.in_column_transform_rule_group_ids ||
                                        all_transform_rule_group_ids.in_column_transform_rule_group_ids
                        FROM    sys_syn.in_groups_def, all_transform_rule_group_ids
                        WHERE   in_groups_def.in_group_id = all_transform_rule_group_ids.parent_in_group_id
                )
                SELECT  in_column_transform_rule_group_ids
                FROM    all_transform_rule_group_ids
                WHERE   parent_in_group_id IS NULL
        );

        _primary_key := (
                SELECT  pg_index.indkey
                FROM    pg_catalog.pg_index
                WHERE   pg_index.indrelid = relation AND
                        pg_index.indisprimary
        );

        FOR     _column IN
        SELECT  *
        FROM    pg_catalog.pg_attribute
        WHERE   pg_attribute.attrelid = relation AND
                pg_attribute.attnum > 0 AND
                NOT pg_attribute.attisdropped AND
                (limit_to_columns IS NULL OR pg_attribute.attname  = ANY(limit_to_columns)) AND
                (omit_columns     IS NULL OR pg_attribute.attname != ALL(omit_columns))
        ORDER BY pg_attribute.attnum
        LOOP

                IF full_prepull_id IS NOT NULL AND _column.attname = 'trans_id_in' THEN
                        _in_column_type := 'TransIdIn'::sys_syn.in_column_type;
                ELSIF key_columns IS NOT NULL THEN
                        IF _column.attname = ANY(key_columns) THEN
                                _in_column_type := 'Key'::sys_syn.in_column_type;
                        ELSE
                                _in_column_type := 'Attribute'::sys_syn.in_column_type;
                        END IF;
                ELSIF _primary_key IS NOT NULL THEN
                        IF _column.attnum = ANY(_primary_key) THEN
                                _in_column_type := 'Key'::sys_syn.in_column_type;
                        ELSE
                                _in_column_type := 'Attribute'::sys_syn.in_column_type;
                        END IF;
                ELSE
                        IF _column.attnotnull THEN
                                _in_column_type := 'Key'::sys_syn.in_column_type;
                        ELSE
                                _in_column_type := 'Attribute'::sys_syn.in_column_type;
                        END IF;
                END IF;

                IF no_diff_columns IS NOT NULL AND _in_column_type = 'Attribute'::sys_syn.in_column_type THEN
                        IF _column.attname = ANY(no_diff_columns) THEN
                                _in_column_type := 'NoDiff'::sys_syn.in_column_type;
                        END IF;
                END IF;

                _column_name            := _column.attname;
                _source_in_expression   := 'in_source.' || quote_ident(_column.attname);
                _format_type            := format_type(_column.atttypid, _column.atttypmod);
                _create_in_columns      := ARRAY[]::sys_syn.create_in_column[];
                _final_ids              := ARRAY[]::TEXT[];
                _omit                   := FALSE;
                _last_priority          := -1;

                FOR _in_column_transform IN
                SELECT  *
                FROM    sys_syn.in_column_transforms
                WHERE   (in_column_transforms.rule_group_id IS NULL OR
                                (       _in_column_transform_rule_group_ids IS NOT NULL AND
                                        in_column_transforms.rule_group_id = ANY(_in_column_transform_rule_group_ids)
                                )
                        ) AND
                        _in_column_type != 'TransIdIn'::sys_syn.in_column_type
                ORDER BY in_column_transforms.priority
                LOOP

                        IF      (_in_column_transform.data_type_like            IS NULL OR
                                        _format_type            LIKE _in_column_transform.data_type_like) AND
                                (_in_column_transform.relation_name_like        IS NULL OR
                                        relation::text          LIKE _in_column_transform.relation_name_like) AND
                                (_in_column_transform.column_name_like          IS NULL OR
                                        _column_name            LIKE _in_column_transform.column_name_like) AND
                                (_in_column_transform.in_column_type            IS NULL OR
                                        _in_column_type = _in_column_transform.in_column_type)
                                THEN

                                IF _in_column_transform.priority = _last_priority THEN
                                        RAISE EXCEPTION
                                     'More than 1 rule meets the criteria of relation ''%'' column ''%'' on the same priority (%).',
                                                relation::text, _column_name, _in_column_transform.priority
                                                USING HINT =
        'Change one of the rule''s priority.  If multiple rules are activated on the same priority, the code may be indeterminate.';
                                END IF;

                                IF _in_column_transform.final_ids IS NOT NULL AND
                                        _final_ids && _in_column_transform.final_ids THEN
                                        CONTINUE;
                                END IF;

                                _final_ids := array_cat(_final_ids, _in_column_transform.final_ids);
                                _last_priority := _in_column_transform.priority;

                                IF _in_column_transform.new_data_type IS NOT NULL THEN
                                        _format_type := _in_column_transform.new_data_type;
                                END IF;

                                IF _in_column_transform.new_in_column_type IS NOT NULL THEN
                                        _in_column_type := _in_column_transform.new_in_column_type;
                                END IF;

                                IF _in_column_transform.create_in_columns IS NOT NULL THEN
                                        _create_in_columns :=
                                                array_cat(_create_in_columns, _in_column_transform.create_in_columns);
                                END IF;

                                IF _in_column_transform.omit IS NOT NULL THEN
                                        _omit := _in_column_transform.omit;
                                END IF;

                                IF _in_column_transform.new_column_name IS NOT NULL THEN
                                        _column_name := _in_column_transform.new_column_name;
                                END IF;

                                IF _in_column_transform.expression IS NOT NULL THEN
                                        _source_in_expression :=
                                                replace(_in_column_transform.expression, '%1', _source_in_expression);
                                END IF;

                                IF _in_column_transform.final_rule THEN
                                        EXIT;
                                END IF;

                        END IF;

                END LOOP;

                IF NOT _omit THEN
                        IF _sql_buffer IS NULL THEN
                                _sql_buffer := $$SELECT sys_syn.in_table_add (
                schema          => $$||(
                                        SELECT  quote_literal(COALESCE(in_table_add_sql.schema::text, pg_namespace.nspname))
                                        FROM    pg_catalog.pg_namespace JOIN
                                                pg_catalog.pg_class ON
                                                        pg_class.relnamespace = pg_namespace.oid
                                        WHERE   pg_class.oid = relation::oid
                )||$$,
                in_table_id     => $$||(
                                        SELECT  quote_literal(COALESCE(in_table_id, pg_class.relname))
                                        FROM    pg_class
                                        WHERE   pg_class.oid = relation::oid
                )||$$,
                in_group_id     => $$||quote_literal(in_table_add_sql.in_group_id)||$$,
                in_pull_id      => NULL,
                in_columns      => ARRAY[
$$;
                        ELSE
                                _sql_buffer := _sql_buffer || ',
';
                        END IF;

                        _sql_buffer := _sql_buffer || $X$                       $COL$("$X$||
                                _column_name||$X$","$X$||
                                _format_type||$X$",$X$||
                                _in_column_type||$X$,"$X$||
                                replace(_source_in_expression, '"', '\"')||$X$",,,,)$COL$$X$;
                END IF;

                FOR     _create_in_column IN
                SELECT  unnest(_create_in_columns)
                LOOP
                        _sql_buffer := _sql_buffer || $$        '"$$||
                                        _create_in_column::text||$$"'$$;
                END LOOP;

        END LOOP;

        _sql_buffer := _sql_buffer || $$
                ]::sys_syn.create_in_column[],
                full_table_reference    => $$||quote_literal(in_table_add_sql.relation::text)||$$,
                changes_table_reference => NULL,
                full_sql                => NULL,
                changes_sql             => NULL,
                full_pre_sql            => NULL,
                changes_pre_sql         => NULL,
                full_post_sql           => NULL,
                changes_post_sql        => NULL,
                enable_deletes_implied  => TRUE,
                full_prepull_id         => $$||quote_nullable(full_prepull_id)||$$,
                changes_prepull_id      => $$||quote_nullable(changes_prepull_id)||$$
        );$$;

        RETURN _sql_buffer;
END;
$_$;
ALTER FUNCTION sys_syn.in_table_add_sql(
        relation                regclass,
        in_group_id             text,
        schema                  regnamespace,
        key_columns             name[],
        no_diff_columns         name[],
        omit_columns            name[],
        limit_to_columns        name[],
        full_prepull_id         text,
        changes_prepull_id      text,
        in_table_id text) OWNER TO postgres;

CREATE OR REPLACE FUNCTION sys_syn.in_table_columns_add_sql(
        in_table_id text,
        relation regclass DEFAULT NULL::regclass,
        key_columns name[] DEFAULT NULL::name[],
        no_diff_columns name[] DEFAULT NULL::name[],
        omit_columns name[] DEFAULT NULL::name[],
        limit_to_columns name[] DEFAULT NULL::name[])
        RETURNS text AS
$BODY$
DECLARE
        _in_table_def           sys_syn.in_tables_def;
        _relation               regclass;
        _in_column_transform_rule_group_ids text[];
        _sql_buffer             TEXT;
        _column                 pg_catalog.pg_attribute%ROWTYPE;
        _column_name            TEXT;
        _in_column_type         sys_syn.in_column_type;
        _primary_key            int2vector;
        _source_in_expression   TEXT;
        _format_type            TEXT;
        _in_column_transform    sys_syn.in_column_transforms%ROWTYPE;
        _create_in_columns      sys_syn.create_in_column[];
        _create_in_column       sys_syn.create_in_column;
        _omit                   boolean;
BEGIN
        _sql_buffer := NULL;
        _relation := in_table_columns_add_sql.relation;

        _in_table_def := (
                SELECT  in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = in_table_columns_add_sql.in_table_id);

        IF _relation IS NULL THEN
                _relation := _in_table_def.full_table_reference::regclass;
        END IF;

        _in_column_transform_rule_group_ids := (
                WITH RECURSIVE all_transform_rule_group_ids(parent_in_group_id, in_column_transform_rule_group_ids) AS (
                        SELECT  in_groups_def.parent_in_group_id,
                                in_groups_def.in_column_transform_rule_group_ids
                        FROM    sys_syn.in_groups_def
                        WHERE   in_groups_def.in_group_id = _in_table_def.in_group_id
                        UNION ALL
                        SELECT  in_groups_def.parent_in_group_id,
                                in_groups_def.in_column_transform_rule_group_ids ||
                                        all_transform_rule_group_ids.in_column_transform_rule_group_ids
                        FROM    sys_syn.in_groups_def, all_transform_rule_group_ids
                        WHERE   in_groups_def.in_group_id = all_transform_rule_group_ids.parent_in_group_id
                )
                SELECT  in_column_transform_rule_group_ids
                FROM    all_transform_rule_group_ids
                WHERE   parent_in_group_id IS NULL
        );

        _primary_key := (
                SELECT  pg_index.indkey
                FROM    pg_catalog.pg_index
                WHERE   pg_index.indrelid = _relation AND
                        pg_index.indisprimary
        );

        FOR     _column IN
        SELECT  *
        FROM    pg_catalog.pg_attribute
        WHERE   pg_attribute.attrelid = _relation AND
                pg_attribute.attnum > 0 AND
                NOT pg_attribute.attisdropped AND
                (in_table_columns_add_sql.limit_to_columns IS NULL OR
                        pg_attribute.attname  = ANY(in_table_columns_add_sql.limit_to_columns)) AND
                (in_table_columns_add_sql.omit_columns     IS NULL OR
                        pg_attribute.attname != ANY(in_table_columns_add_sql.omit_columns))
        ORDER BY pg_attribute.attnum
        LOOP

                IF in_table_columns_add_sql.key_columns IS NOT NULL THEN
                        IF _column.attname = ANY(in_table_columns_add_sql.key_columns) THEN
                                _in_column_type := 'Key'::sys_syn.in_column_type;
                        ELSE
                                _in_column_type := 'Attribute'::sys_syn.in_column_type;
                        END IF;
                ELSIF _primary_key IS NOT NULL THEN
                        IF _column.attnum = ANY(_primary_key) THEN
                                _in_column_type := 'Key'::sys_syn.in_column_type;
                        ELSE
                                _in_column_type := 'Attribute'::sys_syn.in_column_type;
                        END IF;
                ELSE
                        IF _column.attnotnull THEN
                                _in_column_type := 'Key'::sys_syn.in_column_type;
                        ELSE
                                _in_column_type := 'Attribute'::sys_syn.in_column_type;
                        END IF;
                END IF;

                IF in_table_columns_add_sql.no_diff_columns IS NOT NULL AND
                        _in_column_type = 'Attribute'::sys_syn.in_column_type THEN
                        IF _column.attname = ANY(in_table_columns_add_sql.no_diff_columns) THEN
                                _in_column_type := 'NoDiff'::sys_syn.in_column_type;
                        END IF;
                END IF;

                _column_name            := _column.attname;
                _source_in_expression   := 'in_source.' || quote_ident(_column.attname);
                _format_type            := format_type(_column.atttypid, _column.atttypmod);
                _create_in_columns      := ARRAY[]::sys_syn.create_in_column[];
                _omit                   := FALSE;

                FOR _in_column_transform IN
                SELECT  *
                FROM    sys_syn.in_column_transforms
                WHERE   (in_column_transforms.rule_group_id IS NULL OR
                                (       _in_column_transform_rule_group_ids IS NOT NULL AND
                                        in_column_transforms.rule_group_id = ANY(_in_column_transform_rule_group_ids)
                                )
                        )
                ORDER BY in_column_transforms.priority
                LOOP

                        IF      (_in_column_transform.data_type_like            IS NULL OR
                                        _format_type            LIKE _in_column_transform.data_type_like) AND
                                (_in_column_transform.relation_name_like        IS NULL OR
                                        _relation::text         LIKE _in_column_transform.relation_name_like) AND
                                (_in_column_transform.column_name_like          IS NULL OR
                                        _column_name            LIKE _in_column_transform.column_name_like) AND
                                (_in_column_transform.in_column_type            IS NULL OR
                                        _in_column_type = _in_column_transform.in_column_type)
                                THEN

                                IF _in_column_transform.new_data_type IS NOT NULL THEN
                                        _format_type = _in_column_transform.new_data_type;
                                END IF;

                                IF _in_column_transform.new_in_column_type IS NOT NULL THEN
                                        _in_column_type = _in_column_transform.new_in_column_type;
                                END IF;

                                IF _in_column_transform.create_in_columns IS NOT NULL THEN
                                        _create_in_columns =
                                                array_cat(_create_in_columns, _in_column_transform.create_in_columns);
                                END IF;

                                IF _in_column_transform.omit IS NOT NULL THEN
                                        _omit = _in_column_transform.omit;
                                END IF;

                                IF _in_column_transform.new_column_name IS NOT NULL THEN
                                        _column_name = _in_column_transform.new_column_name;
                                END IF;

                                IF _in_column_transform.expression IS NOT NULL THEN
                                        _source_in_expression :=
                                                replace(_in_column_transform.expression, '%1', _source_in_expression);
                                END IF;

                                IF _in_column_transform.final_rule THEN
                                        EXIT;
                                END IF;

                        END IF;

                END LOOP;

                IF EXISTS (
                        SELECT
                        FROM    sys_syn.in_table_columns_def
                        WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                in_table_columns_def.column_name = _column_name
                        ) THEN
                        _omit := TRUE;
                END IF;

                IF NOT _omit THEN
                        IF _sql_buffer IS NULL THEN
                                _sql_buffer := $$SELECT sys_syn.in_table_columns_add (
                $$||quote_literal(_in_table_def.in_table_id)||$$,
                ARRAY[
$$;
                        ELSE
                                _sql_buffer := _sql_buffer || ',
';
                        END IF;

                        _sql_buffer := _sql_buffer || $X$                       $COL$("$X$||
                                _column_name||$X$","$X$||
                                _format_type||$X$",$X$||
                                _in_column_type||$X$,"$X$||
                                replace(_source_in_expression, '"', '\"')||$X$",,,,)$COL$$X$;
                END IF;

                FOR     _create_in_column IN
                SELECT  unnest(_create_in_columns)
                LOOP
                        _sql_buffer := _sql_buffer || $$        '"$$||
                                        _create_in_column::text||$$"'$$;
                END LOOP;

        END LOOP;

        _sql_buffer := _sql_buffer || $$
                ]::sys_syn.create_in_column[]
        );$$;

        RETURN _sql_buffer;
END;
$BODY$
        LANGUAGE plpgsql VOLATILE
        COST 10;
ALTER FUNCTION sys_syn.in_table_columns_add_sql(text, regclass, name[], name[], name[], name[])
        OWNER TO postgres;

CREATE OR REPLACE FUNCTION sys_syn.in_table_columns_add(
        in_table_id text,
        in_columns sys_syn.create_in_column[])
        RETURNS void AS
$BODY$
DECLARE
        _in_table_def           sys_syn.in_tables_def;
        _sql_buffer             TEXT;
        _sql_delimit            BOOLEAN;
        _in_column              sys_syn.create_in_column;
        _column_index           SMALLINT;
        _foreign_key_id         TEXT;
        _foreign_key_index      SMALLINT;
        _primary_column_type    sys_syn.in_column_type;
        _out_table_def          sys_syn.out_tables_def;
BEGIN
        _in_table_def := (
                SELECT  in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = in_table_columns_add.in_table_id);

        _column_index := (
                SELECT  MAX(column_index)
                FROM    sys_syn.in_table_columns_def
                WHERE   in_table_columns_def.in_table_id = in_table_columns_add.in_table_id);

        _sql_buffer := 'ALTER TYPE ' || quote_ident(_in_table_def.schema::text) || '.' || quote_ident(in_table_id||'_in_key') || '
';
        _sql_delimit := FALSE;
        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        WHERE   in_column_rel.in_column_type = 'Key'::sys_syn.in_column_type
        LOOP
                IF _sql_delimit THEN
                        _sql_buffer := _sql_buffer || ',
';
                ELSE
                        _sql_delimit = TRUE;
                END IF;

                _sql_buffer := _sql_buffer || '  ADD ATTRIBUTE '||quote_ident(_in_column.column_name)||' '||_in_column.data_type;
        END LOOP;
        _sql_buffer := _sql_buffer || ';
';
        IF _sql_delimit THEN
                RAISE DEBUG '%', _sql_buffer;
                EXECUTE _sql_buffer;
        END IF;

        _sql_buffer := 'ALTER TYPE ' || quote_ident(_in_table_def.schema::text) || '.' || quote_ident(in_table_id||'_in_attributes')
                || '
';
        _sql_delimit := FALSE;
        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        WHERE   in_column_rel.in_column_type = 'Attribute'::sys_syn.in_column_type
        LOOP
                IF _sql_delimit THEN
                        _sql_buffer := _sql_buffer || ',
';
                ELSE
                        _sql_delimit = TRUE;
                END IF;

                _sql_buffer := _sql_buffer || '  ADD ATTRIBUTE '||quote_ident(_in_column.column_name)||' '||_in_column.data_type;
        END LOOP;
        _sql_buffer := _sql_buffer || ';
';
        IF _sql_delimit THEN
                RAISE DEBUG '%', _sql_buffer;
                EXECUTE _sql_buffer;
        END IF;

        _sql_buffer := 'ALTER TYPE ' || quote_ident(_in_table_def.schema::text) || '.' || quote_ident(in_table_id||'_in_no_diff')
                || '
';
        _sql_delimit := FALSE;
        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        WHERE   in_column_rel.in_column_type = 'NoDiff'::sys_syn.in_column_type
        LOOP
                IF _sql_delimit THEN
                        _sql_buffer := _sql_buffer || ',
';
                ELSE
                        _sql_delimit = TRUE;
                END IF;

                _sql_buffer := _sql_buffer || '  ADD ATTRIBUTE '||quote_ident(_in_column.column_name)||' '||_in_column.data_type;
        END LOOP;
        _sql_buffer := _sql_buffer || ';
';
        IF _sql_delimit THEN
                RAISE DEBUG '%', _sql_buffer;
                EXECUTE _sql_buffer;
        END IF;

        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        LOOP
                _column_index := _column_index + 1;

                INSERT INTO sys_syn.in_table_columns_def (
                        in_table_id,    column_index,   column_name,            source_in_expression)
                VALUES (
                        in_table_id,    _column_index,  _in_column.column_name, _in_column.source_in_expression);
        END LOOP;

        _foreign_key_id := '';
        FOR     _in_column IN
        SELECT  *
        FROM    unnest(in_columns) AS in_column_rel
        WHERE   in_column_rel.foreign_key_index         IS NOT NULL OR
                in_column_rel.primary_in_table_id       IS NOT NULL OR
                in_column_rel.primary_column_name       IS NOT NULL
        ORDER BY
                in_column_rel.foreign_key_index,
                in_column_rel.primary_in_table_id
        LOOP
                IF _in_column.primary_in_table_id IS NULL OR _in_column.primary_column_name IS NULL THEN
                        RAISE EXCEPTION 'primary_in_table_id (%) or primary_column_name (%) is null.',
                                _in_column.primary_in_table_id,  _in_column.primary_column_name
                                USING HINT = 'If you are setting a foreign key, set these fields.  If not, set all of the ' ||
                                'foreign key columns for this column to null.';
                END IF;

                IF _foreign_key_id != (_in_column.foreign_key_index || '-' || _in_column.primary_in_table_id) THEN
                        _foreign_key_index := (
                                SELECT  COALESCE(MAX(in_foreign_keys.foreign_key_index), 0) + 1
                                FROM    sys_syn.in_foreign_keys
                                WHERE   in_foreign_keys.primary_table_id = _in_column.primary_in_table_id AND
                                        in_foreign_keys.foreign_table_id = in_table_id);

                        _foreign_key_id = _in_column.foreign_key_index || '-' || _in_column.primary_in_table_id;
                END IF;

                _primary_column_type := sys_syn.util_column_name_to_in_column_type(
                        _in_column.primary_in_table_id, _in_column.primary_column_name);

                INSERT INTO sys_syn.in_foreign_keys(
                        primary_table_id,               foreign_table_id,               foreign_key_index,
                        primary_column_name,            foreign_column_name)
                VALUES (
                        _in_column.primary_in_table_id, in_table_id,                    _foreign_key_index,
                        _in_column.primary_column_name, _in_column.column_name);
        END LOOP;

        PERFORM sys_syn.util_in_table_code(_in_table_def);

        PERFORM sys_syn.util_in_pulls_code (
                (SELECT in_pulls_def
                FROM    sys_syn.in_pulls_def
                WHERE   in_pulls_def.in_pull_id = _in_table_def.in_pull_id)
        );

        FOR     _out_table_def IN
        SELECT  *
        FROM    sys_syn.out_tables_def
        WHERE   out_tables_def.in_table_id = _in_table_def.in_table_id AND
                out_tables_def.data_view
        ORDER BY out_tables_def.out_group_id
        LOOP
                PERFORM sys_syn.util_out_table_view(_out_table_def);
        END LOOP;
END;
$BODY$
        LANGUAGE plpgsql VOLATILE
        COST 10;
ALTER FUNCTION sys_syn.in_table_columns_add(text, sys_syn.create_in_column[])
        OWNER TO postgres;

CREATE FUNCTION sys_syn.in_trans_finish() RETURNS void
    LANGUAGE plpgsql COST 500
    AS $_$
BEGIN
        UPDATE  sys_syn.in_trans_log
        SET     finish_time = clock_timestamp()
        WHERE   in_trans_log.trans_id_in = sys_syn.trans_id_get();
END;
$_$;
ALTER FUNCTION sys_syn.in_trans_finish() OWNER TO postgres;

CREATE FUNCTION sys_syn.in_trans_start(
        changes_only    boolean,
        in_table_ids    text[] DEFAULT ARRAY[]::text[],
        pre_pull        boolean DEFAULT FALSE)
        RETURNS void
        LANGUAGE plpgsql
        COST 500
        AS $$
BEGIN
        -- Specifying in_table_ids enables deletes to be generated when the table has 0 rows in it.
        -- If in_table_ids is left NULL, the table must have at least 1 row for action to be taken.

        IF (SELECT COUNT(*) != 1 FROM sys_syn.trans_id_mod) THEN
                INSERT INTO sys_syn.trans_id_mod(
                        trans_id_mod)
                VALUES (
                        (       SELECT  COALESCE(MAX(trans_id_in), 0) - txid_current() + 2
                                FROM    sys_syn.in_trans_log
                        )
                );
        END IF;

        INSERT INTO sys_syn.in_trans_log (
                trans_id_in,            pre_pull,                       changes_only,
                trans_time,             in_table_ids
        ) VALUES (
                sys_syn.trans_id_get(), in_trans_start.pre_pull,        in_trans_start.changes_only,
                CURRENT_TIMESTAMP,      in_trans_start.in_table_ids
        );
END;
$$;
ALTER FUNCTION sys_syn.in_trans_start(changes_only boolean, in_table_ids text[], pre_pull boolean) OWNER TO postgres;

CREATE FUNCTION sys_syn.out_table_add_sql(
        schema                  regnamespace,
        in_table_id             text,
        out_group_id            text,
        omit_columns            name[] DEFAULT NULL::name[],
        limit_to_columns        name[] DEFAULT NULL::name[],
        data_view               boolean         DEFAULT FALSE,
        out_log_lifetime        interval        DEFAULT NULL,
        notification_channel    text            DEFAULT NULL,
        enable_adds             boolean         DEFAULT TRUE,
        enable_changes          boolean         DEFAULT TRUE,
        enable_deletes          boolean         DEFAULT TRUE,
        condition_sql           text            DEFAULT NULL,
        claim_limit_rows        integer         DEFAULT 2147483647,
        claim_queue_count       smallint        DEFAULT NULL,
        claim_fixed_by_key      boolean         DEFAULT false,
        claim_random_sample     smallint        DEFAULT NULL,
        queue_pid_used_age      interval        DEFAULT NULL,
        record_comparison_different     text    DEFAULT NULL,
        record_comparison_same          text    DEFAULT NULL)
        RETURNS text
        LANGUAGE plpgsql COST 10
        AS $_$
DECLARE
        _in_table_def           sys_syn.in_tables_def%ROWTYPE;
        _out_column_transform_rule_group_ids text[];
        _sql_buffer             TEXT;
        _in_column              sys_syn.in_table_columns_def%ROWTYPE;
        _in_column_type         sys_syn.in_column_type;
        _data_type              TEXT;
        _out_column_transform   sys_syn.out_column_transforms%ROWTYPE;
        _in_out_column          sys_syn.create_out_column;
        _create_out_columns     sys_syn.create_out_column[];
        _create_out_column      sys_syn.create_out_column;
        _final_ids              text[];
        _omit                   boolean;
        _last_priority          smallint;
BEGIN
        _sql_buffer := NULL;

        _in_table_def := (
                SELECT  in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = out_table_add_sql.in_table_id);

        _out_column_transform_rule_group_ids := (
                WITH RECURSIVE all_transform_rule_group_ids(parent_out_group_id, out_column_transform_rule_group_ids) AS (
                        SELECT  out_groups_def.parent_out_group_id,
                                out_groups_def.out_column_transform_rule_group_ids
                        FROM    sys_syn.out_groups_def
                        WHERE   out_groups_def.out_group_id = out_table_add_sql.out_group_id
                        UNION ALL
                        SELECT  out_groups_def.parent_out_group_id,
                                out_groups_def.out_column_transform_rule_group_ids ||
                                        all_transform_rule_group_ids.out_column_transform_rule_group_ids
                        FROM    sys_syn.out_groups_def, all_transform_rule_group_ids
                        WHERE   out_groups_def.out_group_id = all_transform_rule_group_ids.parent_out_group_id
                )
                SELECT  out_column_transform_rule_group_ids
                FROM    all_transform_rule_group_ids
                WHERE   parent_out_group_id IS NULL
        );

        FOR     _in_column IN
        SELECT  *
        FROM    sys_syn.in_table_columns_def
        WHERE   in_table_columns_def.in_table_id = out_table_add_sql.in_table_id AND
                (limit_to_columns IS NULL OR in_table_columns_def.column_name  = ANY(limit_to_columns)) AND
                (omit_columns     IS NULL OR in_table_columns_def.column_name != ANY(omit_columns))
        UNION ALL
        SELECT  *
        FROM    (
                        VALUES  ('sys_syn_in_queue',-126,NULL::smallint,'sys_syn_trans_id_in',         'trans_id_in'),
                                ('sys_syn_in_queue',-125,NULL,          'sys_syn_delta_type',          'delta_type'),
                                ('sys_syn_in_queue',-124,NULL,          'sys_syn_queue_state',         'queue_state'),
                                ('sys_syn_in_queue',-123,NULL,          'sys_syn_queue_id',            'queue_id'),
                                ('sys_syn_in_queue',-122,NULL,          'sys_syn_queue_priority',      'queue_priority'),
                                ('sys_syn_in_queue',-121,NULL,          'sys_syn_hold_updated',        'hold_updated'),
                                ('sys_syn_in_queue',-120,NULL,          'sys_syn_hold_trans_id_first', 'hold_trans_id_first'),
                                ('sys_syn_in_queue',-119,NULL,          'sys_syn_hold_trans_id_last',  'hold_trans_id_last'),
                                ('sys_syn_in_queue',-118,NULL,          'sys_syn_hold_reason_count',   'hold_reason_count'),
                                ('sys_syn_in_queue',-117,NULL,          'sys_syn_hold_reason_id',      'hold_reason_id'),
                                ('sys_syn_in_queue',-116,NULL,          'sys_syn_hold_reason_text',    'hold_reason_text'),
                                ('sys_syn_in_queue',-115,NULL,          'sys_syn_trans_id_out',        'trans_id_out'),
                                ('sys_syn_in_queue',-114,NULL,          'sys_syn_processed_time',      'processed_time')
                ) AS out_queue_columns_def(in_table_id, column_index, array_order, column_name, source_in_expression)
        WHERE   (limit_to_columns IS NULL OR out_queue_columns_def.column_name  = ANY(limit_to_columns)) AND
                (omit_columns     IS NULL OR out_queue_columns_def.column_name != ALL(omit_columns))
        ORDER BY column_index
        LOOP

                _in_out_column.column_name              := _in_column.column_name;
                _in_out_column.queue_column_name        := NULL;
                _in_out_column.queue_column_expression  := NULL;

                IF _in_column.in_table_id = 'sys_syn_in_queue' THEN
                        _in_column_type         := NULL;
                        _data_type              := NULL;

                        IF EXISTS (
                                SELECT
                                FROM    pg_enum
                                WHERE   enumtypid = 'sys_syn.queue_column'::regtype AND
                                        enumlabel = _in_column.source_in_expression) THEN
                                _in_out_column.queue_column_name        := _in_column.source_in_expression;
                                _in_out_column.queue_column_expression  := 'new.' || _in_column.column_name;
                        END IF;

                        _in_out_column.column_expression := 'out_queue.' || _in_column.source_in_expression;
                ELSE
                        _in_column_type         := sys_syn.util_column_name_to_in_column_type(
                                                        _in_column.in_table_id,
                                                        _in_column.column_name);
                        _data_type              := sys_syn.util_column_name_to_data_type(
                                                        _in_column.in_table_id,
                                                        _in_column.column_name);
                        IF _in_table_def.attributes_array AND _in_column_type = 'Attribute'::sys_syn.in_column_type THEN
                                _in_out_column.column_expression        = 'in_attributes.' ||
                                        quote_ident(_in_column.column_name);
                        ELSE
                                _in_out_column.column_expression        = '(in_source.' ||
                                        sys_syn.util_in_column_type_to_column_name(_in_column_type) || ').' ||
                                        quote_ident(_in_column.column_name);
                        END IF;
                END IF;
                _create_out_columns             := ARRAY[]::sys_syn.create_out_column[];
                _final_ids                      := ARRAY[]::TEXT[];
                _omit                           := FALSE;
                _last_priority                  := -1;

                FOR     _out_column_transform IN
                SELECT  *
                FROM    sys_syn.out_column_transforms
                WHERE   (out_column_transforms.rule_group_id IS NULL OR
                                (       _out_column_transform_rule_group_ids IS NOT NULL AND
                                        out_column_transforms.rule_group_id = ANY(_out_column_transform_rule_group_ids)
                                )
                        )
                ORDER BY out_column_transforms.priority
                LOOP

                        IF      (_out_column_transform.data_type_like            IS NULL OR
                                        _data_type                      LIKE _out_column_transform.data_type_like) AND
                                (_out_column_transform.in_table_id_like          IS NULL OR
                                        out_table_add_sql.in_table_id   LIKE _out_column_transform.in_table_id_like) AND
                                (_out_column_transform.out_group_id_like         IS NULL OR
                                        out_table_add_sql.out_group_id  LIKE _out_column_transform.out_group_id_like) AND
                                (_out_column_transform.column_name_like          IS NULL OR
                                        _in_out_column.column_name      LIKE _out_column_transform.column_name_like) AND
                                (_out_column_transform.in_column_type            IS NULL OR
                                        _in_column_type                 =    _out_column_transform.in_column_type)
                                THEN

                                IF _out_column_transform.priority = _last_priority THEN
                                        RAISE EXCEPTION
                                     'More than 1 rule meets the criteria of in table ''%'' column ''%'' on the same priority (%).',
                                                _in_table_def.in_table_id, _in_out_column.column_name,
                                                _out_column_transform.priority
                                        USING HINT = 'Change one of the rule''s priority.  If multiple rules activated on the sam'||
                                                'e priority, the disorder may generate indeterminate code.';
                                END IF;

                                IF _out_column_transform.final_ids IS NOT NULL AND
                                        _final_ids && _out_column_transform.final_ids THEN
                                        CONTINUE;
                                END IF;

                                _final_ids := array_cat(_final_ids, _out_column_transform.final_ids);
                                _last_priority := _out_column_transform.priority;

                                IF _out_column_transform.new_data_type IS NOT NULL THEN
                                        _data_type = _out_column_transform.new_data_type;
                                END IF;

                                IF _out_column_transform.create_out_columns IS NOT NULL THEN
                                        FOR     _create_out_column IN
                                        SELECT  *
                                        FROM    unnest(_out_column_transform.create_out_columns)
                                        LOOP
                                                _create_out_column.column_name = replace(_create_out_column.column_name, '%1',
                                                        _in_out_column.column_name);

                                                _create_out_column.column_expression = replace(_create_out_column.column_expression,
                                                        '%1', _in_out_column.column_expression);

                                                _create_out_columns =
                                                        array_append(_create_out_columns, _create_out_column);
                                        END LOOP;
                                END IF;

                                IF _out_column_transform.omit IS NOT NULL THEN
                                        _omit = _out_column_transform.omit;
                                END IF;

                                IF _out_column_transform.new_column_name IS NOT NULL THEN
                                        _in_out_column.column_name = replace(_out_column_transform.new_column_name, '%1',
                                                        _in_out_column.column_name);
                                END IF;

                                IF _out_column_transform.expression IS NOT NULL THEN
                                        _in_out_column.column_expression :=
                                                replace(_out_column_transform.expression, '%1',
                                                        _in_out_column.column_expression);
                                END IF;

                                IF _out_column_transform.final_rule THEN
                                        EXIT;
                                END IF;

                        END IF;

                END LOOP;

                IF NOT _omit THEN
                        _create_out_columns = array_prepend(_in_out_column, _create_out_columns);
                END IF;

                FOR     _create_out_column IN
                SELECT  *
                FROM    unnest(_create_out_columns)
                LOOP

                        IF _sql_buffer IS NULL THEN
                                _sql_buffer := $$SELECT sys_syn.out_table_add (
                schema                  => $$||quote_literal(out_table_add_sql.schema::text)||$$,
                in_table_id             => $$||quote_literal(in_table_id)||$$,
                out_group_id            => $$||quote_literal(out_group_id)||$$,
                out_columns             => ARRAY[
$$;
                        ELSE
                                _sql_buffer := _sql_buffer || ',
';
                        END IF;

                        _sql_buffer := _sql_buffer || $X$                       $COL$("$X$||
                                _create_out_column.column_name||$X$","$X$||
                                replace(_create_out_column.column_expression, '"', '\"')||$X$",$X$||
                                COALESCE(_create_out_column.queue_column_name::text, '')||$X$,$X$||
                                COALESCE('"' || replace(_create_out_column.queue_column_expression, '"', '\"') || '"', '') ||
                                $X$)$COL$$X$;

                END LOOP;

        END LOOP;

        _sql_buffer := _sql_buffer || $$
                ]::sys_syn.create_out_column[],
                data_view               => $$||quote_nullable(data_view)||$$,
                out_log_lifetime        => $$||quote_nullable(out_log_lifetime)||$$,
                notification_channel    => $$||quote_nullable(notification_channel)||$$,
                enable_adds             => $$||quote_nullable(enable_adds)||$$,
                enable_changes          => $$||quote_nullable(enable_changes)||$$,
                enable_deletes          => $$||quote_nullable(enable_deletes)||$$,
                condition_sql           => $$||quote_nullable(condition_sql)||$$,
                claim_limit_rows        => $$||quote_nullable(claim_limit_rows)||$$,
                claim_queue_count       => $$||quote_nullable(claim_queue_count)||$$,
                claim_fixed_by_key      => $$||quote_nullable(claim_fixed_by_key)||$$,
                claim_random_sample     => $$||quote_nullable(claim_random_sample)||$$,
                queue_pid_used_age      => $$||quote_nullable(queue_pid_used_age)||$$,
                record_comparison_different=> $$||quote_nullable(record_comparison_different)||$$,
                record_comparison_same     => $$||quote_nullable(record_comparison_same)||$$
        );$$;

        RETURN _sql_buffer;
END;
$_$;
ALTER FUNCTION sys_syn.out_table_add_sql(
        schema                  regnamespace,
        in_table_id             text,
        out_group_id            text,
        omit_columns            name[],
        limit_to_columns        name[],
        data_view               boolean,
        out_log_lifetime        interval,
        notification_channel    text,
        enable_adds             boolean,
        enable_changes          boolean,
        enable_deletes          boolean,
        condition_sql           text,
        claim_limit_rows        integer,
        claim_queue_count       smallint,
        claim_fixed_by_key      boolean,
        claim_random_sample     smallint,
        queue_pid_used_age      interval,
        record_comparison_different     text,
        record_comparison_same          text

) OWNER TO postgres;

CREATE FUNCTION sys_syn.out_table_add(
        schema                  regnamespace,
        in_table_id             text,
        out_group_id            text,
        out_columns             sys_syn.create_out_column[]     DEFAULT NULL,
        data_view               boolean         DEFAULT FALSE,
        out_log_lifetime        interval        DEFAULT NULL,
        notification_channel    text            DEFAULT NULL,
        enable_adds             boolean         DEFAULT TRUE,
        enable_changes          boolean         DEFAULT TRUE,
        enable_deletes          boolean         DEFAULT TRUE,
        condition_sql           text            DEFAULT NULL,
        claim_limit_rows        integer         DEFAULT 2147483647,
        claim_queue_count       smallint        DEFAULT NULL,
        claim_fixed_by_key      boolean         DEFAULT false,
        claim_random_sample     smallint        DEFAULT NULL,
        queue_pid_used_age      interval        DEFAULT NULL,
        record_comparison_different     text    DEFAULT NULL,
        record_comparison_same          text    DEFAULT NULL)
        RETURNS void
        LANGUAGE plpgsql
        COST 10
        AS $_$
DECLARE
        _in_table_def                   sys_syn.in_tables_def%ROWTYPE;
        _sql_name_table_in              TEXT;
        _sql_name_table_temp            TEXT;
        _sql_name_table_orphaned        TEXT;
        _sql_name_table_locked          TEXT;
        _sql_name_table_queue           TEXT;
        _sql_name_table_baseline        TEXT;
        _sql_name_table_exclude         TEXT;
        _sql_name_table_log             TEXT;
        _sql_name_table_queue_pid       TEXT;
        _sql_name_type_in_key           TEXT;
        _sql_name_temp                  TEXT;
        _sql_name_type_in_attributes    TEXT;
        _sql_name_type_in_no_diff       TEXT;
        _sql_buffer                     TEXT;
        _create_out_column              sys_syn.create_out_column;
BEGIN
        _in_table_def := (
                SELECT  in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = out_table_add.in_table_id);

        INSERT INTO sys_syn.out_tables_def(
                in_table_id,    out_group_id,   schema, data_view,      out_log_lifetime,       notification_channel,
                enable_adds,    enable_changes, enable_deletes, condition_sql,
                claim_limit_rows,       claim_queue_count,      claim_fixed_by_key,     claim_random_sample,    queue_pid_used_age,
                record_comparison_different,    record_comparison_same)
        VALUES (
                in_table_id,    out_group_id,   schema, data_view,      out_log_lifetime,       notification_channel,
                enable_adds,    enable_changes, enable_deletes, condition_sql,
                claim_limit_rows,       claim_queue_count,      claim_fixed_by_key,     claim_random_sample,    queue_pid_used_age,
                record_comparison_different,    record_comparison_same);

        INSERT INTO sys_syn.out_tables_state(
                in_table_id,    out_group_id)
        VALUES (
                in_table_id,    out_group_id);

        _sql_name_table_in              := quote_ident(_in_table_def.schema::text) || '.' || quote_ident(in_table_id||'_in');
        _sql_name_type_in_key           := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in_key');
        _sql_name_type_in_attributes    := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in_attributes') ||
                CASE WHEN _in_table_def.attributes_array THEN '[]' ELSE '' END;
        _sql_name_type_in_no_diff       := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_in_no_diff');

        _sql_name_table_log     := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_'||out_group_id||'_log');
        _sql_name_table_temp    := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_'||out_group_id||'_temp');
        _sql_name_table_queue_pid:= quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_'||out_group_id||'_queue_pid');

        _sql_buffer := 'CREATE UNLOGGED TABLE '||_sql_name_table_temp||' (
        key '||_sql_name_type_in_key||' NOT NULL,
        trans_id_in sys_syn.trans_id NOT NULL
);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_temp_pkey');
        _sql_buffer := 'ALTER TABLE ONLY '||_sql_name_table_temp||'
        ADD CONSTRAINT '||_sql_name_temp||' PRIMARY KEY (key);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_temp_trans_id_in_fkey');
        _sql_buffer := 'ALTER TABLE ONLY '||_sql_name_table_temp||'
        ADD CONSTRAINT '||quote_ident(_sql_name_temp||'_trans_id_in_fkey')||' FOREIGN KEY (trans_id_in, key) REFERENCES '||
                _sql_name_table_in||'(trans_id_in, key) ON UPDATE RESTRICT ON DELETE RESTRICT;
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_table_orphaned := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_'||out_group_id||'_orphaned');
        _sql_buffer := 'CREATE TABLE '||_sql_name_table_orphaned||' (
        key '||_sql_name_type_in_key||' NOT NULL,
        trans_id_in sys_syn.trans_id NOT NULL
);';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_orphaned_pkey');
        _sql_buffer := 'ALTER TABLE ONLY '||_sql_name_table_orphaned||'
        ADD CONSTRAINT '||_sql_name_temp||' PRIMARY KEY (key);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_orphaned_trans_id_in_fkey');
        _sql_buffer := 'ALTER TABLE ONLY '||_sql_name_table_orphaned||'
        ADD CONSTRAINT '||_sql_name_temp||' FOREIGN KEY (trans_id_in, key) REFERENCES '||_sql_name_table_in||
                '(trans_id_in, key) ON UPDATE RESTRICT ON DELETE RESTRICT;
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_table_locked := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_'||out_group_id||'_locked');
        _sql_buffer := 'CREATE TABLE '||_sql_name_table_locked||' (
        key '||_sql_name_type_in_key||' NOT NULL,
        trans_id_in sys_syn.trans_id NOT NULL
);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_locked_pkey');
        _sql_buffer := 'ALTER TABLE ONLY '||_sql_name_table_locked||'
        ADD CONSTRAINT '||_sql_name_temp||' PRIMARY KEY (key);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_locked_trans_id_in_fkey');
        _sql_buffer := 'ALTER TABLE ONLY '||_sql_name_table_locked||'
        ADD CONSTRAINT '||_sql_name_temp||' FOREIGN KEY (trans_id_in, key) REFERENCES '||_sql_name_table_in||
                '(trans_id_in, key) ON UPDATE RESTRICT ON DELETE RESTRICT;
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_table_queue := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_'||out_group_id||'_queue');
        _sql_buffer := 'CREATE TABLE '||_sql_name_table_queue||' (
        key '||_sql_name_type_in_key||' NOT NULL,
        trans_id_in sys_syn.trans_id NOT NULL,
        delta_type sys_syn.delta_type NOT NULL,
        queue_state sys_syn.queue_state NOT NULL,
        queue_id smallint,
        queue_priority smallint,
        hold_updated boolean,
        hold_trans_id_first sys_syn.trans_id,
        hold_trans_id_last sys_syn.trans_id,
        hold_reason_count integer,
        hold_reason_id integer,
        hold_reason_text text,
        trans_id_out sys_syn.trans_id,
        processed_time timestamp with time zone
);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_queue_pkey');
        _sql_buffer := 'ALTER TABLE ONLY '||_sql_name_table_queue||'
        ADD CONSTRAINT '||_sql_name_temp||' PRIMARY KEY (key);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_'||out_group_id||'_queue_update');
        _sql_buffer := $$CREATE FUNCTION $$||_sql_name_temp||$$() RETURNS trigger
        LANGUAGE plpgsql
        AS $BODY$
BEGIN
        RETURN new;
END;
$BODY$;
$$;
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_queue_update');
        _sql_buffer := 'CREATE TRIGGER '||_sql_name_temp||' BEFORE UPDATE ON '||_sql_name_table_queue||
                ' FOR EACH ROW EXECUTE PROCEDURE '||quote_ident(schema::text) || '.' || _sql_name_temp||'();
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_table_baseline := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_'||out_group_id||'_baseline');
        _sql_buffer := 'CREATE TABLE '||_sql_name_table_baseline||' (
        key '||_sql_name_type_in_key||' NOT NULL,
        trans_id_in sys_syn.trans_id NOT NULL
);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_baseline_pkey');
        _sql_buffer := 'ALTER TABLE ONLY '||_sql_name_table_baseline||'
        ADD CONSTRAINT '||_sql_name_temp||' PRIMARY KEY (key);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_baseline_trans_id_in_fkey');
        _sql_buffer := 'ALTER TABLE ONLY '||_sql_name_table_baseline||'
        ADD CONSTRAINT '||_sql_name_temp||' FOREIGN KEY (trans_id_in, key) REFERENCES '||_sql_name_table_in||
                '(trans_id_in, key) ON UPDATE RESTRICT ON DELETE RESTRICT;
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_table_exclude := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_'||out_group_id||'_exclude');
        _sql_buffer := 'CREATE TABLE '||_sql_name_table_exclude||' (
        key '||_sql_name_type_in_key||' NOT NULL
);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(in_table_id||'_'||out_group_id||'_exclude_pkey');
        _sql_buffer := 'ALTER TABLE ONLY '||_sql_name_table_exclude||'
        ADD CONSTRAINT '||_sql_name_temp||' PRIMARY KEY (key);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE TABLE '||_sql_name_table_log||' (
        trans_id_in sys_syn.trans_id NOT NULL,
        key '||_sql_name_type_in_key||' NOT NULL,
        trans_id_out sys_syn.trans_id NOT NULL,
        processed_time timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
        delta_type sys_syn.delta_type NOT NULL,
        queue_id smallint
);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE INDEX ON '||_sql_name_table_log||' (trans_id_in, key);';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE INDEX ON '||_sql_name_table_log||' USING brin(processed_time);';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := sys_syn.util_table_create_run_state()||_sql_name_table_queue_pid||' (
        queue_id        smallint,
        node_id         text                            DEFAULT sys_syn.node_id_local_get() NOT NULL,
        pid             integer                         DEFAULT pg_backend_pid() NOT NULL,
        registered_time timestamp with time zone        DEFAULT clock_timestamp() NOT NULL,
        last_used       timestamp with time zone        DEFAULT clock_timestamp() NOT NULL,
        PRIMARY KEY (node_id, pid),
        UNIQUE (queue_id)
);
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_name_temp := quote_ident(schema::text) || '.' || quote_ident(in_table_id||'_'||out_group_id||'_priority');
        _sql_buffer := 'CREATE OR REPLACE FUNCTION '||_sql_name_temp||'(key '||_sql_name_type_in_key||
                ', delta_type sys_syn.delta_type, attributes_new '||_sql_name_type_in_attributes||', no_diff_new '||
                _sql_name_type_in_no_diff||', attributes_baseline '||_sql_name_type_in_attributes||')
  RETURNS smallint AS
$BODY$
BEGIN
        RETURN NULL;
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 20;
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        FOR     _create_out_column IN
        SELECT  *
        FROM    unnest(out_columns)
        LOOP

                INSERT INTO sys_syn.out_view_columns_def (
                        in_table_id,    out_group_id,
                        column_index,
                        column_name,                    column_expression,
                        queue_column_name,      queue_column_expression)
                VALUES (in_table_id,    out_group_id,
                        (
                                SELECT  COALESCE(MAX(out_view_columns_def.column_index), 0) + 1
                                FROM    sys_syn.out_view_columns_def
                                WHERE   out_view_columns_def.in_table_id = out_table_add.in_table_id AND
                                        out_view_columns_def.out_group_id = out_table_add.out_group_id
                        ),
                        _create_out_column.column_name, _create_out_column.column_expression,
                        _create_out_column.queue_column_name,   _create_out_column.queue_column_expression);

        END LOOP;

        PERFORM sys_syn.util_out_table_code(
                (SELECT out_tables_def
                FROM    sys_syn.out_tables_def
                WHERE   out_tables_def.in_table_id      = out_table_add.in_table_id AND
                        out_tables_def.out_group_id     = out_table_add.out_group_id)
        );

        PERFORM sys_syn.util_in_table_code(_in_table_def);
END;
$_$;
ALTER FUNCTION sys_syn.out_table_add(
        schema                  regnamespace,
        in_table_id             text,
        out_group_id            text,
        out_columns             sys_syn.create_out_column[],
        data_view               boolean,
        out_log_lifetime        interval,
        notification_channel    text,
        enable_adds             boolean,
        enable_changes          boolean,
        enable_deletes          boolean,
        condition_sql           text,
        claim_limit_rows        integer,
        claim_queue_count       smallint,
        claim_fixed_by_key      boolean,
        claim_random_sample     smallint,
        queue_pid_used_age      interval,
        record_comparison_different     text,
        record_comparison_same          text) OWNER TO postgres;

CREATE FUNCTION sys_syn.trans_id_get() RETURNS sys_syn.trans_id
        LANGUAGE sql STABLE COST 50
        AS $$
        SELECT (txid_current() + trans_id_mod)::sys_syn.trans_id FROM sys_syn.trans_id_mod;
$$;
ALTER FUNCTION sys_syn.trans_id_get() OWNER TO postgres;

CREATE FUNCTION sys_syn.util_column_name_to_data_type(in_table_id text, column_name name) RETURNS text
        LANGUAGE plpgsql STABLE
        AS $_$
DECLARE
        _in_table_def           sys_syn.in_tables_def;
        _in_column_type_name    name;
        _data_type              text;
BEGIN
        _data_type := null;

        _in_table_def := (
                SELECT  in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = util_column_name_to_data_type.in_table_id);

        _in_column_type_name := in_table_id || '_in_key';
        _data_type := (
                SELECT  format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
                FROM    pg_catalog.pg_namespace JOIN
                        pg_catalog.pg_class ON
                                pg_class.relnamespace = pg_namespace.oid JOIN
                        pg_catalog.pg_attribute ON
                                (pg_attribute.attrelid = pg_class.oid)
                WHERE   pg_namespace.nspname = _in_table_def.schema::text AND
                        pg_class.relname = _in_column_type_name AND
                        pg_attribute.attnum > 0 AND
                        NOT pg_attribute.attisdropped AND
                        pg_attribute.attname = util_column_name_to_data_type.column_name);
        IF _data_type IS NOT NULL THEN
                RETURN _data_type;
        END IF;

        _in_column_type_name := in_table_id || '_in_attributes';
        _data_type := (
                SELECT  format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
                FROM    pg_catalog.pg_namespace JOIN
                        pg_catalog.pg_class ON
                                pg_class.relnamespace = pg_namespace.oid JOIN
                        pg_catalog.pg_attribute ON
                                (pg_attribute.attrelid = pg_class.oid)
                WHERE   pg_namespace.nspname = _in_table_def.schema::text AND
                        pg_class.relname = _in_column_type_name AND
                        pg_attribute.attnum > 0 AND
                        NOT pg_attribute.attisdropped AND
                        pg_attribute.attname = util_column_name_to_data_type.column_name);
        IF _data_type IS NOT NULL THEN
                RETURN _data_type;
        END IF;

        _in_column_type_name := in_table_id || '_in_no_diff';
        _data_type := (
                SELECT  format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
                FROM    pg_catalog.pg_namespace JOIN
                        pg_catalog.pg_class ON
                                pg_class.relnamespace = pg_namespace.oid JOIN
                        pg_catalog.pg_attribute ON
                                (pg_attribute.attrelid = pg_class.oid)
                WHERE   pg_namespace.nspname = _in_table_def.schema::text AND
                        pg_class.relname = _in_column_type_name AND
                        pg_attribute.attnum > 0 AND
                        NOT pg_attribute.attisdropped AND
                        pg_attribute.attname = util_column_name_to_data_type.column_name);
        IF _data_type IS NOT NULL THEN
                RETURN _data_type;
        END IF;

        IF _data_type IS NULL THEN
                IF column_name = 'trans_id_in' THEN
                        RETURN 'sys_syn.trans_id';
                END IF;

                RAISE EXCEPTION 'A column named "%" does not exist in table %.', column_name, in_table_id
                USING HINT = 'Check the column_name and the in_table_id.';
        END IF;
END;
$_$;
ALTER FUNCTION sys_syn.util_column_name_to_data_type(in_table_id text, column_name name) OWNER TO postgres;

CREATE OR REPLACE FUNCTION sys_syn.util_column_name_to_in_column_type(in_table_id text, column_name name)
        RETURNS sys_syn.in_column_type
        LANGUAGE plpgsql STABLE
        AS $_$
DECLARE
        _in_table_def           sys_syn.in_tables_def;
        _in_column_type_name    name;
        _in_column_type         sys_syn.in_column_type;
BEGIN
        _in_column_type := null;

        _in_table_def := (
                SELECT  in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = util_column_name_to_in_column_type.in_table_id);

        _in_column_type_name := in_table_id || '_in_key';
        _in_column_type := (
                SELECT  'Key'::sys_syn.in_column_type
                FROM    pg_catalog.pg_namespace JOIN
                        pg_catalog.pg_class ON
                                pg_class.relnamespace = pg_namespace.oid JOIN
                        pg_catalog.pg_attribute ON
                                (pg_attribute.attrelid = pg_class.oid)
                WHERE   pg_namespace.nspname = _in_table_def.schema::text AND
                        pg_class.relname = _in_column_type_name AND
                        pg_attribute.attnum > 0 AND
                        NOT pg_attribute.attisdropped AND
                        pg_attribute.attname = util_column_name_to_in_column_type.column_name);
        IF _in_column_type IS NOT NULL THEN
                RETURN _in_column_type;
        END IF;

        _in_column_type_name := in_table_id || '_in_attributes';
        _in_column_type := (
                SELECT  'Attribute'::sys_syn.in_column_type
                FROM    pg_catalog.pg_namespace JOIN
                        pg_catalog.pg_class ON
                                pg_class.relnamespace = pg_namespace.oid JOIN
                        pg_catalog.pg_attribute ON
                                (pg_attribute.attrelid = pg_class.oid)
                WHERE   pg_namespace.nspname = _in_table_def.schema::text AND
                        pg_class.relname = _in_column_type_name AND
                        pg_attribute.attnum > 0 AND
                        NOT pg_attribute.attisdropped AND
                        pg_attribute.attname = util_column_name_to_in_column_type.column_name);
        IF _in_column_type IS NOT NULL THEN
                RETURN _in_column_type;
        END IF;

        _in_column_type_name := in_table_id || '_in_no_diff';
        _in_column_type := (
                SELECT  'NoDiff'::sys_syn.in_column_type
                FROM    pg_catalog.pg_namespace JOIN
                        pg_catalog.pg_class ON
                                pg_class.relnamespace = pg_namespace.oid JOIN
                        pg_catalog.pg_attribute ON
                                (pg_attribute.attrelid = pg_class.oid)
                WHERE   pg_namespace.nspname = _in_table_def.schema::text AND
                        pg_class.relname = _in_column_type_name AND
                        pg_attribute.attnum > 0 AND
                        NOT pg_attribute.attisdropped AND
                        pg_attribute.attname = util_column_name_to_in_column_type.column_name);
        IF _in_column_type IS NOT NULL THEN
                RETURN _in_column_type;
        END IF;

        IF _in_column_type IS NULL THEN
                IF column_name = 'trans_id_in' THEN
                        RETURN 'TransIdIn'::sys_syn.in_column_type;
                END IF;

                RAISE EXCEPTION 'A column named "%" does not exist in table %.', column_name, in_table_id
                USING HINT = 'Check the column_name and the in_table_id.';
        END IF;
END;
$_$;
ALTER FUNCTION sys_syn.util_column_name_to_in_column_type(in_table_id text, column_name name) OWNER TO postgres;

CREATE FUNCTION sys_syn.util_in_column_type_to_column_name(in_column_type sys_syn.in_column_type) RETURNS text
        LANGUAGE plpgsql IMMUTABLE COST 10
        AS $$
DECLARE
        _column_name TEXT;
BEGIN
        _column_name :=
                CASE in_column_type
                        WHEN 'Key'::sys_syn.in_column_type              THEN 'key'
                        WHEN 'Attribute'::sys_syn.in_column_type        THEN 'attributes'
                        WHEN 'NoDiff'::sys_syn.in_column_type           THEN 'no_diff'
                        ELSE                                            NULL
                END;

        IF _column_name IS NULL THEN
                RAISE EXCEPTION
                        'The sys_syn.in_column_type % is not known to the function sys_syn.util_in_column_type_to_column_name.',
                        in_column_type::text
                USING HINT = 'Edit this function to handle is value.';
        END IF;

        RETURN _column_name;
END;
$$;
ALTER FUNCTION sys_syn.util_in_column_type_to_column_name(in_column_type sys_syn.in_column_type) OWNER TO postgres;

CREATE FUNCTION sys_syn.util_in_pulls_code(in_pull_def sys_syn.in_pulls_def) RETURNS void
        LANGUAGE plpgsql
        AS $_$
DECLARE
        _in_pull_id_literal             TEXT;
        _function_name_ident            TEXT;
        _in_table_def                   sys_syn.in_tables_def;
        _in_table_column_def            sys_syn.in_table_columns_def;
        _in_table_ident                 TEXT;
        _sql_buffer                     TEXT;
        _first_column                   BOOLEAN;
        _trans_id_source_sql            TEXT;
BEGIN
        _in_pull_id_literal             := quote_literal(in_pull_def.in_pull_id);

        _function_name_ident    := quote_ident(in_pull_def.schema::text) || '.' ||
                quote_ident(in_pull_def.in_pull_id || '_pull');
        _sql_buffer := $$
CREATE OR REPLACE FUNCTION $$||_function_name_ident||$$(changes_only boolean)
  RETURNS BOOLEAN AS
$DEFINITION$
DECLARE
        _in_pull_def            sys_syn.in_pulls_def%ROWTYPE;
        _in_pull_state          sys_syn.in_pulls_state%ROWTYPE;
        _possible_changes       BOOLEAN = FALSE;
BEGIN
        _in_pull_def := (
                SELECT  in_pulls_def
                FROM    sys_syn.in_pulls_def
                WHERE   in_pull_id = $$||_in_pull_id_literal||$$);

        IF NOT pg_try_advisory_lock('sys_syn.in_pulls_def'::regclass::int, _in_pull_def.lock_id) THEN
                RAISE NOTICE 'Pull % is running.', _in_pull_def.in_pull_id;
                RETURN FALSE;
        END IF;

        _in_pull_state := (
                        SELECT  in_pulls_state
                        FROM    sys_syn.in_pulls_state
                        WHERE   in_pulls_state.in_pull_id = _in_pull_def.in_pull_id);

        PERFORM sys_syn.in_trans_start(changes_only, $$ || (
                        SELECT  'ARRAY[' ||
                                        COALESCE(
                                                array_to_string(array_agg(quote_literal(in_table_id) ORDER BY in_pull_order), ','),
                                                '') ||
                                        ']::TEXT[]'
                        FROM    sys_syn.in_tables_def
                        WHERE   in_tables_def.in_pull_id = in_pull_def.in_pull_id AND
                                in_tables_def.full_prepull_id IS NULL
                ) || $$);
$$;

        IF in_pull_def.pull_pre_sql IS NOT NULL THEN
                _sql_buffer := _sql_buffer || $$
$$ || in_pull_def.pull_pre_sql || $$
$$;
        END IF;

        _sql_buffer := _sql_buffer || $$
        IF $$ || quote_ident(in_pull_def.in_pull_id || '_pull') || $$.changes_only THEN
$$;

        FOR     _in_table_def IN
        SELECT  *
        FROM    sys_syn.in_tables_def
        WHERE   in_tables_def.in_pull_id = in_pull_def.in_pull_id
        ORDER BY in_tables_def.in_pull_order
        LOOP

                _in_table_ident         := quote_ident(_in_table_def.schema::text) || '.' ||
                        quote_ident(_in_table_def.in_table_id||'_in');

                IF EXISTS (
                        SELECT
                        FROM    sys_syn.in_table_columns_def
                        WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                sys_syn.util_column_name_to_in_column_type(
                                        in_table_columns_def.in_table_id,
                                        in_table_columns_def.column_name) = 'TransIdIn'::sys_syn.in_column_type
                        ) THEN
                        _trans_id_source_sql := 'in_source.trans_id_in';
                ELSE
                        _trans_id_source_sql := 'sys_syn.trans_id_get()';
                END IF;

                IF _in_table_def.changes_pre_sql IS NOT NULL THEN
                        _sql_buffer := _sql_buffer || $$
$$ || _in_table_def.changes_pre_sql || $$
$$;
                END IF;

                IF _in_table_def.changes_table_reference IS NOT NULL THEN

                        IF _in_table_def.attributes_array THEN

                                _sql_buffer := _sql_buffer || $$
                INSERT  INTO $$||_in_table_ident||$$ (
                        trans_id_in$$;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) != ALL(ARRAY['Attribute'::sys_syn.in_column_type])
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        _sql_buffer := _sql_buffer || ',
                        ' || sys_syn.util_in_column_type_to_column_name(
                                sys_syn.util_column_name_to_in_column_type(
                                        _in_table_column_def.in_table_id,
                                        _in_table_column_def.column_name)
                                ) || '.' || quote_ident(_in_table_column_def.column_name);

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$,
                        attributes)
                SELECT  $$ || _trans_id_source_sql;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) != 'Attribute'::sys_syn.in_column_type
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        _sql_buffer := _sql_buffer || ',
                        ' || COALESCE(_in_table_column_def.source_in_expression, 'in_source.' ||
                                        quote_ident(_in_table_column_def.column_name));

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$,
                        array_agg(
                                ROW(
                                        $$;

                                _first_column := TRUE;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) = 'Attribute'::sys_syn.in_column_type
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        IF _first_column THEN
                                                _first_column := FALSE;
                                        ELSE
                                                _sql_buffer := _sql_buffer || ',
                                        ';
                                        END IF;

                                        _sql_buffer := _sql_buffer || COALESCE(_in_table_column_def.source_in_expression,
                                                'in_source.' || quote_ident(_in_table_column_def.column_name));

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$)::$$ || quote_ident(_in_table_def.schema::text) || $$.$$ ||
                                        quote_ident(_in_table_def.in_table_id || '_in_attributes') || $$
                                ORDER BY
                                        $$;

                                _first_column := TRUE;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        array_order IS NOT NULL
                                ORDER BY in_table_columns_def.array_order
                                LOOP

                                        IF _first_column THEN
                                                _first_column := FALSE;
                                        ELSE
                                                _sql_buffer := _sql_buffer || ',
                                        ';
                                        END IF;

                                        _sql_buffer := _sql_buffer || COALESCE(_in_table_column_def.source_in_expression,
                                                'in_source.' || quote_ident(_in_table_column_def.column_name));

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$)
                FROM    $$||_in_table_def.changes_table_reference||$$ AS in_source
                GROUP BY 1$$;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) != 'Attribute'::sys_syn.in_column_type
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        _sql_buffer := _sql_buffer || ',
                        ' || quote_ident(_in_table_column_def.column_name);

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$;
                IF FOUND THEN _possible_changes = TRUE; END IF;
$$;

                        ELSE
                                _sql_buffer := _sql_buffer || $$
                INSERT  INTO $$||_in_table_ident||$$ (
                        trans_id_in$$;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) != 'TransIdIn'::sys_syn.in_column_type
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        _sql_buffer := _sql_buffer || ',
                        ' || sys_syn.util_in_column_type_to_column_name(
                                sys_syn.util_column_name_to_in_column_type(
                                        _in_table_column_def.in_table_id,
                                        _in_table_column_def.column_name)
                                ) || '.' || quote_ident(_in_table_column_def.column_name);

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$)
                SELECT  $$ || _trans_id_source_sql;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        _sql_buffer := _sql_buffer || ',
                        ' || COALESCE(_in_table_column_def.source_in_expression, 'in_source.' ||
                                        quote_ident(_in_table_column_def.column_name));

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$
                FROM    $$||_in_table_def.changes_table_reference||$$ AS in_source;
                IF FOUND THEN _possible_changes = TRUE; END IF;
$$;
                        END IF;

                END IF;

                IF _in_table_def.changes_sql IS NOT NULL THEN
                        _sql_buffer := _sql_buffer || $$
$$ || _in_table_def.changes_sql || $$
$$;
                END IF;

                IF EXISTS (
                        SELECT
                        FROM    sys_syn.in_table_columns_def
                        WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                in_table_columns_def.column_name = 'sys_syn_delete') THEN
                        _sql_buffer := _sql_buffer || $$
                UPDATE $$||_in_table_ident||$$ SET attributes = NULL WHERE (no_diff).sys_syn_delete;
$$;
                END IF;

                IF _in_table_def.changes_post_sql IS NOT NULL THEN
                        _sql_buffer := _sql_buffer || $$
$$ || _in_table_def.changes_post_sql || $$
$$;
                END IF;

        END LOOP;

        _sql_buffer := _sql_buffer || $$
        ELSE
$$;

        FOR     _in_table_def IN
        SELECT  *
        FROM    sys_syn.in_tables_def
        WHERE   in_tables_def.in_pull_id = in_pull_def.in_pull_id
        ORDER BY in_tables_def.in_pull_order
        LOOP

                _in_table_ident         := quote_ident(_in_table_def.schema::text) || '.' ||
                        quote_ident(_in_table_def.in_table_id||'_in');

                IF _in_table_def.full_pre_sql IS NOT NULL THEN
                        _sql_buffer := _sql_buffer || $$
$$ || _in_table_def.full_pre_sql || $$
$$;
                END IF;

                IF _in_table_def.full_table_reference IS NOT NULL THEN

                        IF _in_table_def.attributes_array THEN

                                _sql_buffer := _sql_buffer || $$
                INSERT  INTO $$||_in_table_ident||$$ (
                        trans_id_in$$;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) != 'Attribute'::sys_syn.in_column_type
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        _sql_buffer := _sql_buffer || ',
                        ' || sys_syn.util_in_column_type_to_column_name(
                                sys_syn.util_column_name_to_in_column_type(
                                        _in_table_column_def.in_table_id,
                                        _in_table_column_def.column_name)
                                ) || '.' || quote_ident(_in_table_column_def.column_name);

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$,
                        attributes)
                SELECT  $$ || _trans_id_source_sql;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) != 'Attribute'::sys_syn.in_column_type
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        _sql_buffer := _sql_buffer || ',
                        ' || COALESCE(_in_table_column_def.source_in_expression, 'in_source.' ||
                                        quote_ident(_in_table_column_def.column_name));

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$,
                        array_agg(
                                ROW(
                                        $$;

                                _first_column := TRUE;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) = 'Attribute'::sys_syn.in_column_type
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        IF _first_column THEN
                                                _first_column := FALSE;
                                        ELSE
                                                _sql_buffer := _sql_buffer || ',
                                        ';
                                        END IF;

                                        _sql_buffer := _sql_buffer || COALESCE(_in_table_column_def.source_in_expression,
                                                'in_source.' || quote_ident(_in_table_column_def.column_name));

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$)::$$ || quote_ident(_in_table_def.schema::text) || $$.$$ ||
                                        quote_ident(_in_table_def.in_table_id || '_in_attributes') || $$
                                ORDER BY
                                        $$;

                                _first_column := TRUE;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        array_order IS NOT NULL
                                ORDER BY in_table_columns_def.array_order
                                LOOP

                                        IF _first_column THEN
                                                _first_column := FALSE;
                                        ELSE
                                                _sql_buffer := _sql_buffer || ',
                                        ';
                                        END IF;

                                        _sql_buffer := _sql_buffer || COALESCE(_in_table_column_def.source_in_expression,
                                                'in_source.' || quote_ident(_in_table_column_def.column_name));

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$)
                FROM    $$||_in_table_def.full_table_reference||$$ AS in_source
                GROUP BY 1$$;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) != 'Attribute'::sys_syn.in_column_type
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        _sql_buffer := _sql_buffer || ',
                        ' || quote_ident(_in_table_column_def.column_name);

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$;
                IF FOUND THEN _possible_changes = TRUE; END IF;
$$;

                        ELSE
                                _sql_buffer := _sql_buffer || $$
                INSERT  INTO $$||_in_table_ident||$$ (
                        trans_id_in$$;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) != 'TransIdIn'::sys_syn.in_column_type
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        _sql_buffer := _sql_buffer || ',
                        ' || sys_syn.util_in_column_type_to_column_name(
                                sys_syn.util_column_name_to_in_column_type(
                                        _in_table_column_def.in_table_id,
                                        _in_table_column_def.column_name)
                                ) || '.' || quote_ident(_in_table_column_def.column_name);

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$)
                SELECT  $$ || _trans_id_source_sql;

                                FOR     _in_table_column_def IN
                                SELECT  *
                                FROM    sys_syn.in_table_columns_def
                                WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                        sys_syn.util_column_name_to_in_column_type(
                                                in_table_columns_def.in_table_id,
                                                in_table_columns_def.column_name) != 'TransIdIn'::sys_syn.in_column_type
                                ORDER BY in_table_columns_def.column_index
                                LOOP

                                        _sql_buffer := _sql_buffer || ',
                        ' || COALESCE(_in_table_column_def.source_in_expression, 'in_source.' ||
                                        quote_ident(_in_table_column_def.column_name));

                                END LOOP;

                                _sql_buffer := _sql_buffer || $$
                FROM    $$||_in_table_def.full_table_reference||$$ AS in_source;
                IF FOUND THEN _possible_changes = TRUE; END IF;
$$;
                        END IF;

                END IF;

                IF _in_table_def.full_sql IS NOT NULL THEN
                        _sql_buffer := _sql_buffer || $$
$$ || _in_table_def.full_sql || $$
$$;
                END IF;

                IF EXISTS (
                        SELECT
                        FROM    sys_syn.in_table_columns_def
                        WHERE   in_table_columns_def.in_table_id = _in_table_def.in_table_id AND
                                in_table_columns_def.column_name = 'sys_syn_delete') THEN
                        _sql_buffer := _sql_buffer || $$
                UPDATE $$||_in_table_ident||$$ SET attributes = NULL WHERE (no_diff).sys_syn_delete;
$$;
                END IF;

                IF _in_table_def.full_post_sql IS NOT NULL THEN
                        _sql_buffer := _sql_buffer || $$
$$ || _in_table_def.full_post_sql || $$
$$;
                END IF;

        END LOOP;

        _sql_buffer := _sql_buffer || $$
        END IF;
$$;

        IF in_pull_def.pull_post_sql IS NOT NULL THEN
                _sql_buffer := _sql_buffer || $$
$$ || in_pull_def.pull_post_sql || $$
$$;
        END IF;

        _sql_buffer := _sql_buffer || $$
        PERFORM sys_syn.in_trans_finish();

        _in_pull_state.last_pull_start  := CURRENT_TIMESTAMP;
        _in_pull_state.last_pull_finish := clock_timestamp();

        IF NOT changes_only THEN
                _in_pull_state.last_pull_full_start     := _in_pull_state.last_pull_start;
                _in_pull_state.last_pull_full_finish    := _in_pull_state.last_pull_finish;
        END IF;

        UPDATE  sys_syn.in_pulls_state
        SET     last_pull_start         = _in_pull_state.last_pull_start,
                last_pull_finish        = _in_pull_state.last_pull_finish,
                last_pull_full_start    = _in_pull_state.last_pull_full_start,
                last_pull_full_finish   = _in_pull_state.last_pull_full_finish
        WHERE   in_pulls_state.in_pull_id = _in_pull_def.in_pull_id;

        PERFORM pg_advisory_unlock('sys_syn.in_pulls_def'::regclass::int, _in_pull_def.lock_id);

        RETURN _possible_changes;
END;
$DEFINITION$
        LANGUAGE plpgsql VOLATILE
        COST 2000;
$$;
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;
END;
$_$;
ALTER FUNCTION sys_syn.util_in_pulls_code(in_pull_def sys_syn.in_pulls_def) OWNER TO postgres;

CREATE FUNCTION sys_syn.util_in_table_code(in_table_def sys_syn.in_tables_def) RETURNS void
        LANGUAGE plpgsql
        AS $_$
DECLARE
        _in_table_id_literal    TEXT;
        _in_table_ident         TEXT;
        _function_name_ident    TEXT;
        _out_table_def          sys_syn.out_tables_def%ROWTYPE;
        _sql_buffer             TEXT;
BEGIN
        _in_table_id_literal    := quote_literal(in_table_def.in_table_id);
        _in_table_ident         := quote_ident(in_table_def.schema::text) || '.' || quote_ident(in_table_def.in_table_id||'_in');

        _function_name_ident    := quote_ident(in_table_def.schema::text) || '.' ||
                quote_ident(in_table_def.in_table_id||'_delete_unmoved');
        _sql_buffer := $$
CREATE OR REPLACE FUNCTION $$||_function_name_ident||$$(delete_unmoved_trans_id_in sys_syn.trans_id)
        RETURNS void AS
$DEFINITION$
DECLARE
        _out_trans_id_in_latest_all     INT;
        _out_trans_id_in_latest_any     INT;
        _in_trans_id_latest_full        INT;
BEGIN
        SELECT  MIN(out_tables_state.trans_id_in_latest),       MAX(out_tables_state.trans_id_in_latest)
        INTO    _out_trans_id_in_latest_all,                    _out_trans_id_in_latest_any
        FROM    sys_syn.out_tables_def JOIN
                sys_syn.out_tables_state USING (out_group_id)
        WHERE   out_tables_def.in_table_id = $$||_in_table_id_literal||$$;

        _in_trans_id_latest_full := (
                SELECT  MAX(in_trans_log.trans_id_in)
                FROM    sys_syn.in_trans_log
                WHERE   (       EXISTS (
                                        SELECT
                                        FROM    $$||_in_table_ident||$$ AS in_data
                                        WHERE   in_data.trans_id_in = in_trans_log.trans_id_in
                                ) OR
                                array_position(in_trans_log.in_table_ids, $$||_in_table_id_literal||$$) IS NOT NULL
                        ) AND
                        NOT in_trans_log.pre_pull AND
                        NOT in_trans_log.changes_only AND
                        in_trans_log.finish_time IS NOT NULL AND
                        in_trans_log.trans_id_in > _out_trans_id_in_latest_any
        );

        IF delete_unmoved_trans_id_in > _out_trans_id_in_latest_any THEN
                DELETE
                FROM    $$||_in_table_ident||$$ AS in_data
                WHERE   trans_id_in >= delete_unmoved_trans_id_in;
        ELSE
                DELETE
                FROM    $$||_in_table_ident||$$ AS in_data
                WHERE   trans_id_in >= delete_unmoved_trans_id_in$$ ||
                                sys_syn.util_out_tables_exists_code(in_table_def, 3::smallint) || $$;
        END IF;
END;
$DEFINITION$
        LANGUAGE plpgsql VOLATILE
        COST 5000;
$$;
        EXECUTE _sql_buffer;

        _function_name_ident := quote_ident(in_table_def.schema::text) || '.' || quote_ident(in_table_def.in_table_id||'_vacuum');
        _sql_buffer := $$
CREATE OR REPLACE FUNCTION $$||_function_name_ident||
                                                  $$(preserve_trans_id_in_and_after sys_syn.trans_id DEFAULT NULL::sys_syn.trans_id)
        RETURNS void AS
$DEFINITION$
DECLARE
        _out_trans_id_in_latest_all     INT;
        _out_trans_id_in_latest_any     INT;
        _in_trans_id_latest_full        INT;
        _in_trans_id_remove_before      INT;
BEGIN$$;

        FOR     _out_table_def IN
        SELECT  *
        FROM    sys_syn.out_tables_def
        WHERE   out_tables_def.in_table_id = util_in_table_code.in_table_def.in_table_id
        ORDER BY out_tables_def.out_group_id
        LOOP
                _sql_buffer := _sql_buffer || $$
DELETE FROM $$ || quote_ident(_out_table_def.schema::text) || '.' ||
        quote_ident(_out_table_def.in_table_id||'_'||_out_table_def.out_group_id||'_log') || $$
WHERE   processed_time < CURRENT_TIMESTAMP - (

                SELECT  COALESCE(out_log_lifetime, INTERVAL '0 days')
                FROM    sys_syn.out_tables_def
                WHERE   out_tables_def.in_table_id      = $$ || quote_literal(_out_table_def.in_table_id) || $$ AND
                        out_tables_def.out_group_id     = $$ || quote_literal(_out_table_def.out_group_id) || $$);
$$;
        END LOOP;

        _sql_buffer := _sql_buffer || $$
        SELECT  MIN(out_tables_state.trans_id_in_latest),       MAX(out_tables_state.trans_id_in_latest)
        INTO    _out_trans_id_in_latest_all,                    _out_trans_id_in_latest_any
        FROM    sys_syn.out_tables_def JOIN
                sys_syn.out_tables_state USING (out_group_id)
        WHERE   out_tables_def.in_table_id = $$||_in_table_id_literal||$$;

        _in_trans_id_latest_full := (
                SELECT  MAX(in_trans_log.trans_id_in)
                FROM    sys_syn.in_trans_log
                WHERE   (       EXISTS (
                                        SELECT
                                        FROM    $$||_in_table_ident||$$ AS in_data
                                        WHERE   in_data.trans_id_in = in_trans_log.trans_id_in
                                ) OR
                                array_position(in_trans_log.in_table_ids, $$||_in_table_id_literal||$$) IS NOT NULL
                        ) AND
                        NOT in_trans_log.pre_pull AND
                        NOT in_trans_log.changes_only AND
                        in_trans_log.finish_time IS NOT NULL
        );

        IF preserve_trans_id_in_and_after IS NOT NULL THEN
                IF sys_syn.trans_id_get() > preserve_trans_id_in_and_after THEN
                        RAISE EXCEPTION 'The trans_id_in % has not elapsed yet.', preserve_trans_id_in_and_after
                        USING HINT = 'The latest is ' || sys_syn.trans_id_get() || '.  Use a number less than it.';
                ELSIF _out_trans_id_in_latest_all >= preserve_trans_id_in_and_after THEN
                        RAISE EXCEPTION
                        'The trans_id_in % is before the latest completely moved trans_id_in.  This value will have no effect.',
                                preserve_trans_id_in_and_after
                        USING HINT = 'The latest completely read trans_id_in is ' || _out_trans_id_in_latest_all ||
                                '.  Use a number after it.';
                END IF;
        END IF;

        _in_trans_id_remove_before := GREATEST(_out_trans_id_in_latest_all + 1, _in_trans_id_latest_full);

        -- Remove all older than the latest full dataset, or the last dataset read by all outs, whichever is most recent.
        DELETE
        FROM    $$||_in_table_ident||$$ AS in_data
        WHERE   in_data.trans_id_in < _in_trans_id_remove_before AND
                (preserve_trans_id_in_and_after IS NULL OR in_data.trans_id_in <= preserve_trans_id_in_and_after)$$ ||
                sys_syn.util_out_tables_exists_code(in_table_def, 2::smallint) || $$;

        -- Deduplicate the data that has not been read by all outs yet.
        -- If records have multiple changes, delete all but the most recent.
        -- Do not delete from full datasets so that deletes are correctly processed.
        -- The prior query deleted all but the latest full dataset, so only one full dataset should be present.
        DELETE
        FROM    $$||_in_table_ident||$$ AS in_data
        USING   sys_syn.in_trans_log
        WHERE   in_trans_log.trans_id_in = in_data.trans_id_in AND
                NOT in_trans_log.pre_pull AND
                EXISTS (
                        SELECT
                        FROM    (
                                        SELECT DISTINCT ON (key)
                                                trans_id_in,            key
                                        FROM    $$||_in_table_ident||$$ AS changes
                                        WHERE   changes.trans_id_in > _out_trans_id_in_latest_any
                                        ORDER BY key, trans_id_in DESC
                                ) AS most_recent_change
                        WHERE   most_recent_change.trans_id_in  != in_data.trans_id_in AND
                                most_recent_change.key          = in_data.key
                ) AND
                in_data.trans_id_in > _out_trans_id_in_latest_any AND
                (preserve_trans_id_in_and_after IS NULL OR in_data.trans_id_in <= preserve_trans_id_in_and_after) AND
                in_trans_log.changes_only$$ || sys_syn.util_out_tables_exists_code(in_table_def, 2::smallint) || $$;
END;
$DEFINITION$
  LANGUAGE plpgsql VOLATILE
  COST 5000;
$$;
        EXECUTE _sql_buffer;
END;
$_$;
ALTER FUNCTION sys_syn.util_in_table_code(in_table_def sys_syn.in_tables_def) OWNER TO postgres;

CREATE OR REPLACE FUNCTION sys_syn.util_out_tables_for_pro_for_tab_cols_code(
        out_table_def sys_syn.out_tables_def,
        primary_table_id text)
        RETURNS text AS
$BODY$
DECLARE
        _sql_buffer             TEXT;
        _in_foreign_key         sys_syn.in_foreign_keys%ROWTYPE;
BEGIN
        _sql_buffer := '';

        FOR     _in_foreign_key IN
        SELECT  *
        FROM    sys_syn.in_foreign_keys
        WHERE   in_foreign_keys.primary_table_id = util_out_tables_for_pro_for_tab_cols_code.primary_table_id AND
                in_foreign_keys.foreign_table_id = util_out_tables_for_pro_for_tab_cols_code.out_table_def.in_table_id
        ORDER BY foreign_key_index
        LOOP

                IF _sql_buffer = '' THEN
                        _sql_buffer := $$EXISTS (
                        SELECT
                        FROM    $$ || quote_ident(util_out_tables_for_pro_for_tab_cols_code.out_table_def.schema::text) || $$.$$ ||
                                quote_ident(util_out_tables_for_pro_for_tab_cols_code.primary_table_id || '_' ||
                                util_out_tables_for_pro_for_tab_cols_code.out_table_def.out_group_id || '_baseline') ||
                                        $$ AS foreign_out_baseline
                        WHERE   $$;
                ELSE
                        _sql_buffer := _sql_buffer || $$ AND
                                        $$;
                END IF;

                _sql_buffer := _sql_buffer || '(foreign_out_baseline.' ||
                        sys_syn.util_in_column_type_to_column_name(
                                sys_syn.util_column_name_to_in_column_type(
                                        primary_table_id,
                                        _in_foreign_key.primary_column_name)
                        ) || ').' || quote_ident(_in_foreign_key.primary_column_name) || ' = (in_orphaned.' ||
                        sys_syn.util_in_column_type_to_column_name(
                                sys_syn.util_column_name_to_in_column_type(
                                        out_table_def.in_table_id,
                                        _in_foreign_key.foreign_column_name)
                        ) ||').' || quote_ident(_in_foreign_key.foreign_column_name);

        END LOOP;

        _sql_buffer := _sql_buffer || $$
                )$$;

        RETURN _sql_buffer;
END;
$BODY$
        LANGUAGE plpgsql VOLATILE
        COST 100;
ALTER FUNCTION sys_syn.util_out_tables_for_pro_for_tab_cols_code(
        out_table_def sys_syn.out_tables_def,
        primary_table_id text) OWNER TO postgres;

CREATE OR REPLACE FUNCTION sys_syn.util_out_tables_for_pro_for_tabs_cols_code(out_table_def sys_syn.out_tables_def)
        RETURNS text AS
$BODY$
DECLARE
        _sql_buffer             TEXT;
        _primary_table_id       TEXT;
BEGIN
        _sql_buffer := '';

        FOR     _primary_table_id IN
        SELECT  primary_table_id
        FROM    sys_syn.in_foreign_keys
        WHERE   foreign_table_id = util_out_tables_for_pro_for_tabs_cols_code.out_table_def.in_table_id
        GROUP BY primary_table_id
        ORDER BY primary_table_id
        LOOP

                IF _sql_buffer != '' THEN
                        _sql_buffer := _sql_buffer || $$ AND
                $$;
                END IF;

                _sql_buffer := _sql_buffer || sys_syn.util_out_tables_for_pro_for_tab_cols_code(out_table_def, _primary_table_id);

        END LOOP;

        RETURN _sql_buffer;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION sys_syn.util_out_tables_for_pro_for_tabs_cols_code(out_table_def sys_syn.out_tables_def) OWNER TO postgres;

CREATE FUNCTION sys_syn.util_out_table_view(out_table_def sys_syn.out_tables_def) RETURNS void
        LANGUAGE plpgsql COST 10
        AS $_$
DECLARE
        _in_table_def                   sys_syn.in_tables_def%ROWTYPE;
        _sql_name_out_view              TEXT;
        _sql_name_out_view_rule_update  TEXT;
        _sql_name_out_queue             TEXT;
        _sql_name_in                    TEXT;
        _sql_buffer                     TEXT;
        _out_view_column_def            sys_syn.out_view_columns_def;
        _out_view_column_first          BOOLEAN;
BEGIN
        _in_table_def := (
                SELECT  in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = out_table_def.in_table_id);

        _sql_name_out_view := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id||'_'||out_table_def.out_group_id||'_queue_data');
        _sql_name_out_view_rule_update :=
                quote_ident(out_table_def.in_table_id||'_'||out_table_def.out_group_id||'_queue_data_update');
        _sql_name_out_queue := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id||'_'||out_table_def.out_group_id||'_queue');
        _sql_name_in := quote_ident(_in_table_def.schema::text) || '.' ||
                quote_ident(_in_table_def.in_table_id||'_in');
        _sql_buffer := 'DROP VIEW IF EXISTS '||_sql_name_out_view||';

CREATE VIEW '||_sql_name_out_view||' AS
SELECT  ';

        IF EXISTS (
                SELECT
                FROM    sys_syn.out_view_columns_def
                WHERE   out_view_columns_def.in_table_id = out_table_def.in_table_id AND
                        out_view_columns_def.out_group_id = out_table_def.out_group_id) THEN

                _sql_buffer := _sql_buffer || 'out_queue.key AS sys_syn_key';

                FOR     _out_view_column_def IN
                SELECT  *
                FROM    sys_syn.out_view_columns_def
                WHERE   out_view_columns_def.in_table_id = out_table_def.in_table_id AND
                        out_view_columns_def.out_group_id = out_table_def.out_group_id
                ORDER BY out_view_columns_def.column_index
                LOOP

                        _sql_buffer := _sql_buffer || ',
        ' || _out_view_column_def.column_expression || ' AS ' || quote_ident(_out_view_column_def.column_name);

                END LOOP;

        ELSE

                _sql_buffer := _sql_buffer ||
       'out_queue.key                   AS sys_syn_key,
        out_queue.trans_id_in           AS sys_syn_trans_id_in,
        out_queue.delta_type            AS sys_syn_delta_type,
        out_queue.queue_state           AS sys_syn_queue_state,
        out_queue.queue_id              AS sys_syn_queue_id,
        out_queue.queue_priority        AS sys_syn_queue_priority,
        out_queue.hold_updated          AS sys_syn_hold_updated,
        out_queue.hold_trans_id_first   AS sys_syn_hold_trans_id_first,
        out_queue.hold_trans_id_last    AS sys_syn_hold_trans_id_last,
        out_queue.hold_reason_count     AS sys_syn_hold_reason_count,
        out_queue.hold_reason_id        AS sys_syn_hold_reason_id,
        out_queue.hold_reason_text      AS sys_syn_hold_reason_text,
        out_queue.trans_id_out          AS sys_syn_trans_id_out,
        out_queue.processed_time        AS sys_syn_processed_time,
        (in_source.key).*,
        ' || CASE WHEN _in_table_def.attributes_array THEN 'in_attributes.*' ELSE '(in_source.attributes).*' END ||',
        (in_source.no_diff).*';

        END IF;

        _sql_buffer := _sql_buffer || '
FROM    '||_sql_name_out_queue||' AS out_queue LEFT OUTER JOIN
                '||_sql_name_in||' AS in_source USING (trans_id_in, key)';

        IF _in_table_def.attributes_array THEN
                _sql_buffer := _sql_buffer || ',
        unnest(in_source.attributes) AS in_attributes';
        END IF;

        _sql_buffer := _sql_buffer || ';

CREATE RULE '||_sql_name_out_view_rule_update||' AS ON UPDATE TO '||_sql_name_out_view||' DO INSTEAD (
        UPDATE  '||_sql_name_out_queue||'
        SET     ';

        IF EXISTS (
                SELECT
                FROM    sys_syn.out_view_columns_def
                WHERE   out_view_columns_def.in_table_id = out_table_def.in_table_id AND
                        out_view_columns_def.out_group_id = out_table_def.out_group_id AND
                        out_view_columns_def.queue_column_name IS NOT NULL) THEN

                _out_view_column_first = TRUE;

                FOR     _out_view_column_def IN
                SELECT  *
                FROM    sys_syn.out_view_columns_def
                WHERE   out_view_columns_def.in_table_id = out_table_def.in_table_id AND
                        out_view_columns_def.out_group_id = out_table_def.out_group_id AND
                        out_view_columns_def.queue_column_name IS NOT NULL
                ORDER BY out_view_columns_def.column_index
                LOOP

                        IF _out_view_column_first THEN
                                _out_view_column_first = FALSE;
                        ELSE
                                _sql_buffer := _sql_buffer || ',
                ';
                        END IF;

                        _sql_buffer := _sql_buffer ||
                                _out_view_column_def.queue_column_name::text || ' = ' ||
                                        _out_view_column_def.queue_column_expression;

                END LOOP;

        ELSE

                _sql_buffer := _sql_buffer ||
               'queue_state     = NEW.sys_syn_queue_state,
                queue_id        = NEW.sys_syn_queue_id,
                queue_priority  = NEW.sys_syn_queue_priority,
                hold_reason_id  = NEW.sys_syn_hold_reason_id,
                hold_reason_text= NEW.sys_syn_hold_reason_text,
                processed_time  = NEW.sys_syn_processed_time';

        END IF;

        _sql_buffer := _sql_buffer || '
        WHERE   key = OLD.sys_syn_key;
);';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;
END;
$_$;
ALTER FUNCTION sys_syn.util_out_table_view(out_table_def sys_syn.out_tables_def) OWNER TO postgres;

CREATE FUNCTION sys_syn.util_record_comparison_code(
        left_table_alias        text,
        right_table_alias       text,
        out_table_def           sys_syn.out_tables_def,
        attributes_different    boolean)
        RETURNS text
        LANGUAGE plpgsql
        AS $_$
DECLARE
        _in_table_def                   sys_syn.in_tables_def;
        _record_comparison_different    TEXT;
        _record_comparison_same         TEXT;
BEGIN
        _in_table_def := (
                SELECT  in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = out_table_def.in_table_id);

        IF _in_table_def.attributes_array THEN
                _record_comparison_different    := 'row(%1.attributes)::record *<> row(%2.attributes)::record';
                _record_comparison_same         := 'row(%1.attributes)::record *= row(%2.attributes)::record';
        ELSE
                _record_comparison_different    := '%1.attributes      *<> %2.attributes';
                _record_comparison_same         := '%1.attributes      *= %2.attributes';
        END IF;

        -- Using IS [ NOT ] DISTINCT FROM will cause data loss in the destination system when values have the same identity, but
        -- they also have a different representation (case insensitive text, as an example).  The commented out code below is an
        -- example of what NOT to use if you want your destination system to be kept up-to-date.  You may, however, get away with
        -- using the below if you restrict all attribute data types to the basics (text, int, bigint, smallint, date, timestamptz,
        -- etc.)  Do NOT use:
        --_record_comparison_same         := '%1.attributes      IS NOT DISTINCT FROM %2.attributes';
        --_record_comparison_different    := '%1.attributes      IS DISTINCT FROM %2.attributes';
        -- Also do not use = and <> / != for the same reason.  However, = is worse as NULLs will return NULL instead of true/false.

        _record_comparison_different    := COALESCE(
                                                out_table_def.record_comparison_different,
                                                _in_table_def.record_comparison_different,
                                                _record_comparison_different);
        _record_comparison_same         := COALESCE(
                                                out_table_def.record_comparison_same,
                                                _in_table_def.record_comparison_same,
                                                '(NOT ' || COALESCE(
                                                        out_table_def.record_comparison_different,
                                                        _in_table_def.record_comparison_different) || ')',
                                                _record_comparison_same);

        IF attributes_different THEN
                _record_comparison_same := _record_comparison_different;
        END IF;

        RETURN replace(
                replace(_record_comparison_same, '%2', quote_ident(right_table_alias)),
                '%1',
                quote_ident(left_table_alias));
END;
$_$;
ALTER FUNCTION sys_syn.util_record_comparison_code(
        left_table_alias        text,
        right_table_alias       text,
        out_table_def           sys_syn.out_tables_def,
        attributes_different    boolean) OWNER TO postgres;

CREATE FUNCTION sys_syn.util_out_table_code(out_table_def sys_syn.out_tables_def) RETURNS void
        LANGUAGE plpgsql
        AS $_$
DECLARE
        _in_table_def                   sys_syn.in_tables_def;
        _in_table_id_literal            TEXT;
        _in_table_ident                 TEXT;
        _in_table_exclude_ident         TEXT;
        _out_group_id_literal           TEXT;
        _out_table_baseline_ident       TEXT;
        _out_table_locked_ident         TEXT;
        _out_table_orphaned_ident       TEXT;
        _out_table_queue_ident          TEXT;
        _out_table_temp_ident           TEXT;
        _out_table_exclude_ident        TEXT;
        _out_table_queue_pid_ident      TEXT;
        _out_table_log_ident            TEXT;
        _out_proc_priority_ident        TEXT;
        _function_name_ident            TEXT;
        _sql_buffer                     TEXT;
BEGIN
        _in_table_def := (
                SELECT  in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = out_table_def.in_table_id);

        _in_table_id_literal            := quote_literal(_in_table_def.in_table_id);
        _in_table_ident                 := quote_ident(_in_table_def.schema::text) || '.' ||
                quote_ident(_in_table_def.in_table_id||'_in');
        _in_table_exclude_ident         := quote_ident(_in_table_def.schema::text) || '.' ||
                quote_ident(_in_table_def.in_table_id||'_exclude');
        _out_group_id_literal           := quote_literal(out_table_def.out_group_id);
        _out_table_baseline_ident       := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_baseline');
        _out_table_locked_ident := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_locked');
        _out_table_orphaned_ident       := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_orphaned');
        _out_table_queue_ident          := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_queue');
        _out_table_temp_ident           := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_temp');
        _out_table_exclude_ident        := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_exclude');
        _out_table_queue_pid_ident      := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_queue_pid');
        _out_table_log_ident            := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_log');
        _out_proc_priority_ident        := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_priority');

        IF EXISTS (
                SELECT
                FROM    sys_syn.in_foreign_keys
                WHERE   foreign_table_id = out_table_def.in_table_id) THEN

                _function_name_ident    := quote_ident(out_table_def.schema::text) || '.' ||
                        quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_foreign_processed');
                _sql_buffer := $$
CREATE OR REPLACE FUNCTION $$||_function_name_ident||$$() RETURNS BOOLEAN
        LANGUAGE plpgsql COST 500
        AS $DEFINITION$
DECLARE
        _possible_changes       BOOLEAN = FALSE;
BEGIN
        IF NOT (
                SELECT  out_tables_def.enable_adds
                FROM    sys_syn.out_tables_def
                WHERE   in_table_id     = $$||quote_literal(out_table_def.in_table_id)||$$ AND
                        out_group_id    = $$||quote_literal(out_table_def.out_group_id)||$$) THEN
                RETURN false;
        END IF;

        INSERT INTO $$||_out_table_queue_ident||$$ (
                key,                            trans_id_in,
                delta_type,
                queue_state,
                queue_priority
        )
        SELECT  out_orphaned.key,               out_orphaned.trans_id_in,
                'Add'::sys_syn.delta_type,
                'Unread'::sys_syn.queue_state,
                $$||_out_proc_priority_ident||$$(out_orphaned.key, 'Add'::sys_syn.delta_type, NULL, NULL, NULL)
        FROM    $$||_out_table_orphaned_ident||$$ AS out_orphaned,
                $$||_in_table_ident||$$           AS in_orphaned
        WHERE   in_orphaned.trans_id_in     = out_orphaned.trans_id_in AND
                in_orphaned.key             = out_orphaned.key AND
                $$||sys_syn.util_out_tables_for_pro_for_tabs_cols_code(out_table_def)||$$
        ON CONFLICT DO NOTHING; -- Continue if the user manually put rows into the queue and forgot about the orphaned state table.
        IF FOUND THEN _possible_changes = TRUE; END IF;

        DELETE
        FROM    $$||_out_table_orphaned_ident||$$ AS out_orphaned
        WHERE   EXISTS (
                        SELECT
                        FROM    $$||_out_table_queue_ident||$$ AS out_queue
                        WHERE   out_queue.key = out_orphaned.key
                );

        RETURN _possible_changes;
END;
$DEFINITION$;
$$;
                EXECUTE _sql_buffer;
        END IF;

        _function_name_ident    := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_move');
        _sql_buffer := $$
CREATE OR REPLACE FUNCTION $$||_function_name_ident||
                                                 $$(latest_trans_id sys_syn.trans_id DEFAULT NULL::sys_syn.trans_id) RETURNS BOOLEAN
        LANGUAGE plpgsql COST 500
        AS $DEFINITION$
DECLARE
        _latest_trans_id        sys_syn.trans_id;
        _trans_id_start         sys_syn.trans_id;
        _trans_id_end           sys_syn.trans_id;
        _out_table_state        sys_syn.out_tables_state%ROWTYPE;
        _out_table_def          sys_syn.out_tables_def%ROWTYPE;
        _changes_only           BOOLEAN;
        _possible_changes       BOOLEAN = FALSE;
BEGIN
        _out_table_def := (
                SELECT  out_tables_def
                FROM    sys_syn.out_tables_def
                WHERE   in_table_id     = $$||_in_table_id_literal||$$ AND
                        out_group_id    = $$||_out_group_id_literal||$$);

        IF NOT pg_try_advisory_lock('sys_syn.out_tables_def'::regclass::int, _out_table_def.lock_id) THEN
                RAISE EXCEPTION 'The table move for %_% is already running.', _out_table_def.in_table_id,
                        _out_table_def.out_group_id
                USING HINT = 'Wait for the existing move to finish before calling it again.';
        END IF;

        --LOCK TABLE $$||_out_table_queue_ident||$$ IN ACCESS EXCLUSIVE MODE;
        PERFORM
        FROM    $$||_out_table_queue_ident||$$ AS out_queue
        WHERE   queue_state = 'Unread'::sys_syn.queue_state
        FOR UPDATE;

        _out_table_state := (
                SELECT  out_tables_state
                FROM    sys_syn.out_tables_state
                WHERE   in_table_id     = $$||_in_table_id_literal||$$ AND
                        out_group_id    = $$||_out_group_id_literal||$$);

        _trans_id_start := _out_table_state.trans_id_in_latest + 1;
        _latest_trans_id := latest_trans_id;

        IF _latest_trans_id IS NULL THEN
                _latest_trans_id := sys_syn.trans_id_get();
        ELSIF _latest_trans_id > sys_syn.trans_id_get() THEN
                RAISE EXCEPTION 'The latest_trans_id % has not elapsed yet.', latest_trans_id
                USING HINT = 'Use the default value if you do not have a valid trans_id.  The latest is ' || sys_syn.trans_id_get()
                        || '.';
        END IF;

        -- Find the most recent full dataset.
        _trans_id_end := (
                        SELECT  MAX(in_trans_log.trans_id_in)
                        FROM    sys_syn.in_trans_log
                        WHERE   (       EXISTS (
                                                SELECT
                                                FROM    $$||_in_table_ident||$$ AS in_data
                                                WHERE   in_data.trans_id_in = in_trans_log.trans_id_in
                                        ) OR
                                        array_position(in_trans_log.in_table_ids, $$||_in_table_id_literal||$$) IS NOT NULL
                                ) AND
                                NOT in_trans_log.changes_only AND
                                in_trans_log.finish_time IS NOT NULL AND
                                in_trans_log.trans_id_in >= _trans_id_start AND
                                in_trans_log.trans_id_in <= _latest_trans_id);

        IF _trans_id_end IS NULL THEN
                _changes_only   := TRUE;
                SELECT  MIN(in_trans_log.trans_id_in),  MAX(in_trans_log.trans_id_in)
                INTO    _trans_id_start,                _trans_id_end
                FROM    $$||_in_table_ident||$$ JOIN
                                sys_syn.in_trans_log USING (trans_id_in)
                WHERE   in_trans_log.trans_id_in >= _trans_id_start AND
                        in_trans_log.trans_id_in <= _latest_trans_id AND
                        in_trans_log.finish_time IS NOT NULL;
        ELSE
                _changes_only   := FALSE;
                _trans_id_start := _trans_id_end;
        END IF;

        TRUNCATE $$||_out_table_temp_ident||$$;

        INSERT INTO $$||_out_table_temp_ident||$$
        SELECT DISTINCT ON (in_data.key)
                in_data.key,    in_data.trans_id_in
        FROM    $$||_in_table_ident||$$ AS in_data
        WHERE   in_data.trans_id_in BETWEEN _trans_id_start AND _trans_id_end AND
                NOT EXISTS (
                        SELECT
                        FROM    $$||_in_table_exclude_ident||$$ AS in_exclude
                        WHERE   in_exclude.key = in_data.key
                ) AND
                NOT EXISTS (
                        SELECT
                        FROM    $$||_out_table_exclude_ident||$$ AS out_exclude
                        WHERE   out_exclude.key = in_data.key
                )$$||COALESCE(' AND (
                        ' || out_table_def.condition_sql || ')', '')||$$
        ORDER BY in_data.key, in_data.trans_id_in DESC
        ON CONFLICT (key) DO UPDATE
        SET     trans_id_in = EXCLUDED.trans_id_in;
$$;

        IF EXISTS (
                SELECT
                FROM    sys_syn.in_foreign_keys
                WHERE   foreign_table_id = out_table_def.in_table_id) THEN

                _sql_buffer := _sql_buffer || $$
        PERFORM $$||quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_foreign_processed')||$$();
$$;
        END IF;

        _sql_buffer := _sql_buffer || $$
        IF _changes_only THEN
                -- Delete unchanged data to reduce I/O with the Unread records below.
                DELETE
                FROM    $$||_out_table_temp_ident||$$           AS out_temp
                USING   $$||_in_table_ident||$$                 AS in_temp,
                        $$||_out_table_baseline_ident||$$       AS out_baseline,
                        $$||_in_table_ident||$$                 AS in_baseline
                WHERE   in_temp.trans_id_in     = out_temp.trans_id_in AND
                        in_temp.key             = out_temp.key AND
                        out_baseline.key        = out_temp.key AND
                        in_baseline.trans_id_in = out_baseline.trans_id_in AND
                        in_baseline.key         = out_baseline.key AND
                        $$||sys_syn.util_record_comparison_code('in_temp','in_baseline',out_table_def,false)||$$;
        END IF;

        -- Retry locked records.

        INSERT INTO $$||_out_table_temp_ident||$$
        SELECT  out_locked.key, out_locked.trans_id_in
        FROM    $$||_out_table_locked_ident||$$ AS out_locked
        WHERE   EXISTS (
                        SELECT
                        FROM    $$||_out_table_queue_ident||$$ AS out_queue
                        WHERE   out_queue.key           = out_locked.key AND
                                out_queue.queue_state   = 'Unread'::sys_syn.queue_state
                ) OR
                NOT EXISTS (
                        SELECT
                        FROM    $$||_out_table_queue_ident||$$ AS out_queue
                        WHERE   out_queue.key = out_locked.key
                );

        DELETE
        FROM    $$||_out_table_locked_ident||$$ AS out_locked
        WHERE   EXISTS (
                        SELECT
                        FROM    $$||_out_table_temp_ident||$$ AS out_temp
                        WHERE   out_temp.key = out_locked.key
                );

        -- Remove records from the queue that have been deleted or are about to be replaced.

        IF _changes_only THEN
                DELETE FROM $$||_out_table_queue_ident||$$ AS out_queue
                WHERE   queue_state = 'Unread'::sys_syn.queue_state AND
                        EXISTS (
                                SELECT
                                FROM    $$||_out_table_temp_ident||$$ AS out_temp
                                WHERE   out_temp.key = out_queue.key
                        );
        ELSE
                DELETE FROM $$||_out_table_queue_ident||$$
                WHERE   queue_state = 'Unread'::sys_syn.queue_state;

                TRUNCATE $$||_out_table_orphaned_ident||$$;
        END IF;

        $$ || sys_syn.util_out_tables_orphaned_code(out_table_def) || $$IF NOT _out_table_def.enable_adds THEN
                TRUNCATE $$||_out_table_orphaned_ident||$$;
        END IF;
-- Keep locked records out of the queue for now.

        INSERT INTO $$||_out_table_locked_ident||$$
        SELECT  *
        FROM    $$||_out_table_temp_ident||$$ AS out_temp
        WHERE   EXISTS (
                        SELECT
                        FROM    $$||_out_table_queue_ident||$$ AS out_queue
                        WHERE   out_queue.key = out_temp.key
                );

        DELETE FROM $$||_out_table_temp_ident||$$ AS out_temp
        WHERE   EXISTS (
                        SELECT
                        FROM    $$||_out_table_locked_ident||$$ AS out_locked
                        WHERE   out_locked.key = out_temp.key
                );

        -- Diff Hold (locked) records.

        IF NOT _changes_only THEN
                IF _out_table_def.enable_deletes THEN
                        UPDATE  $$||_out_table_queue_ident||$$ AS out_queue
                        SET     trans_id_in             = _trans_id_start,
                                delta_type              = 'Delete'::sys_syn.delta_type,
                                queue_state             = 'Unread'::sys_syn.queue_state,
                                queue_id                = NULL,
                                queue_priority          = $$||_out_proc_priority_ident||$$(out_queue.key,
                                                                'Delete'::sys_syn.delta_type, NULL, NULL, NULL),
                                hold_updated            = TRUE,
                                hold_trans_id_first     = NULL,
                                hold_trans_id_last      = NULL,
                                hold_reason_count       = NULL,
                                hold_reason_id          = NULL,
                                hold_reason_text        = NULL,
                                trans_id_out            = NULL,
                                processed_time          = NULL
                        WHERE   queue_state             = 'Hold'::sys_syn.queue_state AND
                                NOT EXISTS (
                                        SELECT
                                        FROM    $$||_out_table_locked_ident||$$ AS out_locked
                                        WHERE   out_locked.key = out_queue.key
                                );
                        IF FOUND THEN _possible_changes = TRUE; END IF;
                END IF;
        END IF;

        IF _out_table_def.enable_changes THEN
                UPDATE  $$||_out_table_queue_ident||$$ AS out_queue
                SET     trans_id_in             = in_locked.trans_id_in,
                        delta_type              = 'Change'::sys_syn.delta_type,
                        queue_state             = 'Unread'::sys_syn.queue_state,
                        queue_id                = NULL,
                        queue_priority          = $$||_out_proc_priority_ident||$$(in_data.key, 'Change'::sys_syn.delta_type,
                                                        in_data.attributes, in_data.no_diff, in_locked.attributes),
                        hold_updated            = TRUE,
                        hold_trans_id_first     = NULL,
                        hold_trans_id_last      = NULL,
                        hold_reason_count       = NULL,
                        hold_reason_id          = NULL,
                        hold_reason_text        = NULL,
                        trans_id_out            = NULL,
                        processed_time          = NULL
                FROM    $$||_in_table_ident||$$                 AS in_data,
                        $$||_out_table_locked_ident||$$ AS out_locked,
                        $$||_in_table_ident||$$                 AS in_locked
                WHERE   in_data.trans_id_in     = out_queue.trans_id_in AND
                        in_data.key             = out_queue.key AND
                        out_locked.key          = out_queue.key AND
                        out_queue.queue_state   = 'Hold'::sys_syn.queue_state AND
                        in_locked.trans_id_in   = out_locked.trans_id_in AND
                        in_locked.key           = out_locked.key AND
                        $$||sys_syn.util_record_comparison_code('in_data','in_locked',out_table_def,true)||$$;
                IF FOUND THEN _possible_changes = TRUE; END IF;
        END IF;

        IF _out_table_def.enable_deletes THEN
                -- Explicit deletes
                INSERT INTO $$||_out_table_queue_ident||$$ (
                        key,                    trans_id_in,
                        delta_type,                     queue_state,
                        queue_priority)
                SELECT  out_temp.key,           out_temp.trans_id_in,
                        'Delete'::sys_syn.delta_type,   'Unread'::sys_syn.queue_state,
                        $$||_out_proc_priority_ident||$$(out_temp.key, 'Delete'::sys_syn.delta_type, NULL, NULL, NULL)
                FROM    $$||_out_table_temp_ident||$$ AS out_temp,
                        $$||_in_table_ident||$$       AS in_temp
                WHERE   in_temp.trans_id_in     = out_temp.trans_id_in AND
                        in_temp.key             = out_temp.key AND
                        -- Test if the composite itself is NULL, not any of the values inside of it.
                        ROW(ROW(in_temp.attributes)) IS NULL AND
                        EXISTS (
                                SELECT
                                FROM    $$||_out_table_baseline_ident||$$ AS out_baseline
                                WHERE   out_baseline.key = out_temp.key
                        );
                IF FOUND THEN _possible_changes = TRUE; END IF;
        END IF;

        -- Diff
$$;
        IF _in_table_def.enable_deletes_implied THEN

                _sql_buffer := _sql_buffer || $$
        IF NOT _changes_only AND _out_table_def.enable_deletes THEN
                -- Implicit deletes
                INSERT INTO $$||_out_table_queue_ident||$$ (
                        key,                    trans_id_in,
                        delta_type,                     queue_state,
                        queue_priority)
                SELECT  out_baseline.key,       _trans_id_start,
                        'Delete'::sys_syn.delta_type,   'Unread'::sys_syn.queue_state,
                        $$||_out_proc_priority_ident||$$(out_baseline.key, 'Delete'::sys_syn.delta_type, NULL, NULL, NULL)
                FROM    $$||_out_table_baseline_ident||$$ AS out_baseline
                WHERE   NOT EXISTS (
                                SELECT
                                FROM    $$||_out_table_temp_ident||$$ AS out_temp
                                WHERE   out_temp.key = out_baseline.key
                        ) AND
                        NOT EXISTS (
                                SELECT
                                FROM    $$||_out_table_locked_ident||$$ AS out_locked
                                WHERE   out_locked.key = out_baseline.key
                        );
                IF FOUND THEN _possible_changes = TRUE; END IF;
        END IF;
$$;
        END IF;

        _sql_buffer := _sql_buffer || $$

        IF _out_table_def.enable_changes THEN
                INSERT INTO $$||_out_table_queue_ident||$$ (
                        key,                    trans_id_in,
                        delta_type,                     queue_state,
                        queue_priority)
                SELECT  out_temp.key,           out_temp.trans_id_in,
                        'Change'::sys_syn.delta_type,   'Unread'::sys_syn.queue_state,
                        $$||_out_proc_priority_ident||$$(in_temp.key, 'Change'::sys_syn.delta_type,
                                in_temp.attributes, in_temp.no_diff, in_baseline.attributes)
                FROM    $$||_out_table_temp_ident||$$           AS out_temp,
                        $$||_in_table_ident||$$                 AS in_temp,
                        $$||_out_table_baseline_ident||$$       AS out_baseline,
                        $$||_in_table_ident||$$                 AS in_baseline
                WHERE   in_temp.trans_id_in     = out_temp.trans_id_in AND
                        in_temp.key             = out_temp.key AND
                        out_baseline.key        = out_temp.key AND
                        in_baseline.trans_id_in = out_baseline.trans_id_in AND
                        in_baseline.key         = out_baseline.key AND
                        -- Test if the composite itself is not NULL, not any of the values inside of it.
                        ROW(ROW(in_temp.attributes)) IS NOT NULL AND
                        $$||sys_syn.util_record_comparison_code('in_temp','in_baseline',out_table_def,true)||$$;
                IF FOUND THEN _possible_changes = TRUE; END IF;
        END IF;

        IF _out_table_def.enable_adds THEN
                INSERT INTO $$||_out_table_queue_ident||$$ (
                        key,                    trans_id_in,
                        delta_type,                     queue_state,
                        queue_priority)
                SELECT  out_temp.key,           out_temp.trans_id_in,
                        'Add'::sys_syn.delta_type,      'Unread'::sys_syn.queue_state,
                        $$||_out_proc_priority_ident||$$(out_temp.key, 'Add'::sys_syn.delta_type, NULL, NULL, NULL)
                FROM    $$||_out_table_temp_ident||$$ AS out_temp
                WHERE   NOT EXISTS (
                                SELECT
                                FROM    $$||_out_table_baseline_ident||$$ AS out_baseline
                                WHERE   out_baseline.key = out_temp.key
                        );
                IF FOUND THEN _possible_changes = TRUE; END IF;
        END IF;

        TRUNCATE $$||_out_table_temp_ident||$$;

        -- It would be better to prevent inserts for disabled delta_types, but some code updates the delta_type, so the following
        -- solution keeps the code simple and accurate.
        IF NOT _out_table_def.enable_adds THEN
                DELETE FROM $$||_out_table_queue_ident||$$ AS out_queue
                WHERE   out_queue.delta_type    = 'Add'::sys_syn.delta_type AND
                        out_queue.queue_state   = 'Unread'::sys_syn.queue_state;
        END IF;

        IF NOT _out_table_def.enable_changes THEN
                DELETE FROM $$||_out_table_queue_ident||$$ AS out_queue
                WHERE   out_queue.delta_type    = 'Change'::sys_syn.delta_type AND
                        out_queue.queue_state   = 'Unread'::sys_syn.queue_state;
        END IF;

        IF NOT _out_table_def.enable_deletes THEN
                DELETE FROM $$||_out_table_queue_ident||$$ AS out_queue
                WHERE   out_queue.delta_type    = 'Delete'::sys_syn.delta_type AND
                        out_queue.queue_state   = 'Unread'::sys_syn.queue_state;
        END IF;

        -- Keep track of the last transaction processed.
        UPDATE  sys_syn.out_tables_state
        SET     trans_id_in_latest = $$ || CASE WHEN _in_table_def.full_prepull_id IS NOT NULL  THEN '_trans_id_end'
                                                                                                ELSE '_latest_trans_id' END || $$
        WHERE   in_table_id     = $$||_in_table_id_literal||$$ AND
                out_group_id    = $$||_out_group_id_literal||$$;
$$;
        IF out_table_def.notification_channel IS NOT NULL THEN

                _sql_buffer := _sql_buffer || $$
        IF _possible_changes THEN
                NOTIFY $$||quote_ident(out_table_def.notification_channel)||$$;
        END IF;
$$;
        END IF;

        _sql_buffer := _sql_buffer || $$
        PERFORM pg_advisory_unlock('sys_syn.out_tables_def'::regclass::int, _out_table_def.lock_id);

        RETURN _possible_changes;
END;
$DEFINITION$;
$$;
        EXECUTE _sql_buffer;

        _function_name_ident    := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_processed');
        _sql_buffer := $$
CREATE OR REPLACE FUNCTION $$||_function_name_ident||$$() RETURNS BOOLEAN
        LANGUAGE plpgsql COST 500
        AS $DEFINITION$
DECLARE
        _out_table_def          sys_syn.out_tables_def%ROWTYPE;
        _processed_delete       BOOLEAN := FALSE;
        _processed_addchange    BOOLEAN := FALSE;
        _unlocked_change        BOOLEAN := FALSE;
BEGIN
        _out_table_def := (
                SELECT  out_tables_def
                FROM    sys_syn.out_tables_def
                WHERE   in_table_id     = $$||_in_table_id_literal||$$ AND
                        out_group_id    = $$||_out_group_id_literal||$$);

        DELETE
        FROM    $$||_out_table_baseline_ident||$$       AS out_baseline
        USING   $$||_out_table_queue_ident||$$  AS out_queue
        WHERE   out_baseline.key        = out_queue.key AND
                out_queue.queue_state   = 'Processed'::sys_syn.queue_state AND
                out_queue.delta_type    = 'Delete'::sys_syn.delta_type;
        IF FOUND THEN _processed_delete = TRUE; END IF;

        DELETE
        FROM    $$||_out_table_queue_ident||$$  AS out_queue
        WHERE   out_queue.queue_state   = 'Processed'::sys_syn.queue_state AND
                out_queue.delta_type    = 'Delete'::sys_syn.delta_type;

        /* If ON CONFLICT (key) DO UPDATE does not perform well:

        UPDATE  $$||_out_table_baseline_ident||$$ AS out_baseline
        SET     trans_id_in = out_queue.trans_id_in
        FROM    $$||_out_table_queue_ident||$$ AS out_queue
        WHERE   out_queue.key = out_baseline.key AND
                out_queue.queue_state = 'Processed'::sys_syn.queue_state;
        IF FOUND THEN _processed_addchange = TRUE; END IF;

        INSERT INTO $$||_out_table_baseline_ident||$$
        SELECT  out_queue.key,          out_queue.trans_id_in
        FROM    $$||_out_table_queue_ident||$$ AS out_queue
        WHERE   out_queue.queue_state = 'Processed'::sys_syn.queue_state AND
                NOT EXISTS (
                        SELECT
                        FROM    $$||_out_table_baseline_ident||$$ AS out_baseline
                        WHERE   out_baseline.key = out_queue.key
                );
        IF FOUND THEN _processed_addchange = TRUE; END IF;*/

        INSERT INTO $$||_out_table_baseline_ident||$$
        SELECT  out_queue.key,          out_queue.trans_id_in
        FROM    $$||_out_table_queue_ident||$$ AS out_queue
        WHERE   out_queue.queue_state = 'Processed'::sys_syn.queue_state
        ON CONFLICT (key) DO UPDATE
        SET     trans_id_in = EXCLUDED.trans_id_in;
        IF FOUND THEN _processed_addchange = TRUE; END IF;

        DELETE
        FROM    $$||_out_table_queue_ident||$$  AS out_queue
        USING   $$||_out_table_baseline_ident||$$       AS out_baseline
        WHERE   out_queue.key           = out_baseline.key AND
                out_queue.trans_id_in   = out_baseline.trans_id_in AND
                out_queue.queue_state   = 'Processed'::sys_syn.queue_state;

        IF _out_table_def.enable_changes THEN
                INSERT INTO $$||_out_table_queue_ident||$$ (
                        key,                    trans_id_in,
                        delta_type,
                        queue_state,
                        queue_priority
                )
                SELECT  out_locked.key,         out_locked.trans_id_in,
                        'Change'::sys_syn.delta_type,
                        'Unread'::sys_syn.queue_state,
                        $$||_out_proc_priority_ident||$$(in_locked.key, 'Change'::sys_syn.delta_type, in_locked.attributes,
                                in_locked.no_diff, baseline_data.attributes)
                FROM    $$||_out_table_locked_ident||$$ AS out_locked JOIN
                        $$||_in_table_ident||$$           AS in_locked ON
                                in_locked.trans_id_in           = out_locked.trans_id_in AND
                                in_locked.key                   = out_locked.key JOIN
                        $$||_out_table_baseline_ident||$$       AS out_baseline ON
                                out_baseline.key        = out_locked.key JOIN
                        $$||_in_table_ident||$$                 AS baseline_data ON
                                baseline_data.key       = out_baseline.key
                WHERE   $$||sys_syn.util_record_comparison_code('in_locked','baseline_data',out_table_def,true)||$$ AND
                        NOT EXISTS (
                                SELECT
                                FROM    $$||_out_table_queue_ident||$$ AS out_queue
                                WHERE   out_queue.key = out_locked.key
                        );
                IF FOUND THEN _unlocked_change = TRUE; END IF;
        END IF;

        DELETE
        FROM    $$||_out_table_locked_ident||$$ AS out_locked
        WHERE   EXISTS (
                        SELECT
                        FROM    $$||_out_table_queue_ident||$$ AS out_queue
                        WHERE   out_queue.key           = out_locked.key AND
                                out_queue.trans_id_in   = out_locked.trans_id_in
                );
$$;
        IF out_table_def.notification_channel IS NOT NULL THEN

                _sql_buffer := _sql_buffer || $$
        IF _unlocked_change THEN
                NOTIFY $$||quote_ident(out_table_def.notification_channel)||$$;
        END IF;
$$;
        END IF;

        _sql_buffer := _sql_buffer || $$
        RETURN _processed_delete OR _processed_addchange OR _unlocked_change;
END;
$DEFINITION$;
$$;
        EXECUTE _sql_buffer;

        _function_name_ident    := quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_queue_update');
        _sql_buffer := $$
CREATE OR REPLACE FUNCTION $$||quote_ident(out_table_def.schema::text) || '.' || _function_name_ident||$$()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $DEFINITION$
DECLARE
        _processed_delete       BOOLEAN := FALSE;
        _processed_addchange    BOOLEAN := FALSE;
        _unlocked_change        BOOLEAN := FALSE;
BEGIN
        IF      new.queue_state = 'Hold'::sys_syn.queue_state AND
                new.queue_state IS DISTINCT FROM old.queue_state THEN

                IF new.hold_trans_id_first IS NULL THEN
                        new.hold_trans_id_first := sys_syn.trans_id_get();
                        new.hold_reason_count   := 1;
                ELSE
                        IF      new.hold_reason_id      IS DISTINCT FROM old.hold_reason_id OR
                                new.hold_reason_text    IS DISTINCT FROM old.hold_reason_text THEN
                                new.hold_reason_count   := 1;
                        ELSE
                                new.hold_reason_count   := new.hold_reason_count + 1;
                        END IF;
                END IF;

                new.hold_trans_id_last := sys_syn.trans_id_get();

        ELSIF   new.queue_state = 'Processed'::sys_syn.queue_state AND
                new.queue_state IS DISTINCT FROM old.queue_state THEN

                new.trans_id_out := sys_syn.trans_id_get();

                IF new.processed_time IS NULL THEN
                        new.processed_time := clock_timestamp();
                END IF;
$$;
        IF out_table_def.out_log_lifetime IS NOT NULL THEN
                _sql_buffer := _sql_buffer || $$
                INSERT INTO $$||_out_table_log_ident||$$(
                        trans_id_in,            key,            trans_id_out,
                        processed_time,         delta_type,     queue_id)
                VALUES (new.trans_id_in,        new.key,        new.trans_id_out,
                        new.processed_time,     new.delta_type, new.queue_id);
$$;
        END IF;
        _sql_buffer := _sql_buffer || $$
        END IF;

        RETURN new;
END;
$DEFINITION$;
$$;
        EXECUTE _sql_buffer;

        _function_name_ident    := quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_claim');
        _sql_buffer := $$
CREATE OR REPLACE FUNCTION $$||quote_ident(out_table_def.schema::text) || '.' || _function_name_ident||$$(
        queue_id        smallint,
        limit_rows      integer DEFAULT NULL,
        queue_count     smallint DEFAULT NULL,
        fixed_by_key    boolean DEFAULT false,
        random_sample   smallint DEFAULT NULL)
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        COST 500
        AS $DEFINITION$
DECLARE
        _limit_rows     integer;
        _queue_count    smallint;
        _fixed_by_key   boolean;
        _random_sample  smallint;
        _possible_changes       BOOLEAN = FALSE;
BEGIN
        SELECT  COALESCE($$||_function_name_ident||$$.limit_rows,    out_tables_def.claim_limit_rows),
                COALESCE($$||_function_name_ident||$$.queue_count,   out_tables_def.claim_queue_count),
                COALESCE($$||_function_name_ident||$$.fixed_by_key,  out_tables_def.claim_fixed_by_key),
                COALESCE($$||_function_name_ident||$$.random_sample, out_tables_def.claim_random_sample)
        INTO    _limit_rows,
                _queue_count,
                _fixed_by_key,
                _random_sample
        FROM    sys_syn.out_tables_def
        WHERE   in_table_id     = $$||quote_literal(out_table_def.in_table_id)||$$ AND
                out_group_id    = $$||quote_literal(out_table_def.out_group_id)||$$;

        IF _queue_count IS NOT NULL AND NOT _fixed_by_key THEN
                _limit_rows := LEAST(
                        _limit_rows,
                        ceiling((       SELECT  COUNT(*)
                                        FROM    $$||_out_table_queue_ident||$$ AS out_queue
                                        WHERE   out_queue.queue_state != 'Hold'::sys_syn.queue_state
                                ) / _queue_count::decimal
                                )
                        );
                _queue_count := NULL;
        END IF;

        UPDATE  $$||_out_table_queue_ident||$$ AS out_queue
        SET     queue_state = 'Reading'::sys_syn.queue_state,
                queue_id = $$||_function_name_ident||$$.queue_id
        FROM    (       SELECT  lock_queue.key
                        FROM    $$||_out_table_queue_ident||$$ AS lock_queue
                        WHERE   lock_queue.queue_state = 'Unread'::sys_syn.queue_state AND
                                (       _queue_count IS NULL OR
                                        CAST(CAST(('x0' || RIGHT(MD5(lock_queue.key::text), 7)) AS bit(32)) AS int) % _queue_count =
                                                $$||_function_name_ident||$$.queue_id
                                ) AND
                                (       _random_sample IS NULL OR
                                        random() <= 1::double precision /
                                                _random_sample::double precision)
                        ORDER BY lock_queue.queue_priority NULLS LAST
                        LIMIT   _limit_rows
                        FOR UPDATE SKIP LOCKED
        ) AS record_locks
        WHERE   record_locks.key = out_queue.key;
        IF FOUND THEN _possible_changes = TRUE; END IF;

        IF NOT _possible_changes THEN
                _possible_changes := EXISTS (
                        SELECT
                        FROM    $$||_out_table_queue_ident||$$ AS out_queue
                        WHERE   out_queue.queue_state = 'Reading'::sys_syn.queue_state AND
                                out_queue.queue_id IS NOT DISTINCT FROM $$||_function_name_ident||$$.queue_id
                        LIMIT 1
                );
        END IF;

        RETURN _possible_changes;
END;
$DEFINITION$;
$$;
        EXECUTE _sql_buffer;

        _function_name_ident    := quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_queue_id_claim');
        _sql_buffer := $$
CREATE OR REPLACE FUNCTION $$||quote_ident(out_table_def.schema::text) || '.' || _function_name_ident||$$()
        RETURNS smallint
        LANGUAGE plpgsql
        SECURITY DEFINER
        COST 20
        AS $DEFINITION$
DECLARE
        _queue_id               smallint := NULL;
        _claim_queue_count      smallint := NULL;
BEGIN
        DELETE FROM $$||_out_table_queue_pid_ident||$$ AS queue_pid
        WHERE   queue_pid.node_id = sys_syn.node_id_local_get() AND
                NOT EXISTS (
                        SELECT
                        FROM    pg_catalog.pg_stat_activity
                        WHERE   pg_stat_activity.pid = queue_pid.pid
                );

        SELECT  queue_id
        INTO    _queue_id
        FROM    $$||_out_table_queue_pid_ident||$$ AS id_current
        WHERE   id_current.node_id = sys_syn.node_id_local_get() AND
                id_current.pid = pg_backend_pid();

        IF _queue_id IS NOT NULL THEN
                -- Reduce very frequent write activity for a normally read-only path.
                IF (
                        SELECT  clock_timestamp() - last_used >= '44 seconds'::INTERVAL
                        FROM    $$||_out_table_queue_pid_ident||$$ AS queue_pid
                        WHERE   queue_pid.node_id = sys_syn.node_id_local_get() AND
                                queue_pid.pid = pg_backend_pid()
                        ) THEN
                        UPDATE  $$||_out_table_queue_pid_ident||$$ AS queue_pid
                        SET     last_used = clock_timestamp()
                        WHERE   queue_pid.node_id = sys_syn.node_id_local_get() AND
                                queue_pid.pid = pg_backend_pid();
                END IF;

                RETURN _queue_id;
        END IF;

        SELECT  claim_queue_count
        INTO    _claim_queue_count
        FROM    sys_syn.out_tables_def
        WHERE   in_table_id     = $$||quote_literal(out_table_def.in_table_id)||$$ AND
                out_group_id    = $$||quote_literal(out_table_def.out_group_id)||$$;

        IF _claim_queue_count IS NOT NULL THEN
                SELECT  COUNT(*)
                INTO    _queue_id
                FROM    $$||_out_table_queue_pid_ident||$$;

                IF _claim_queue_count <= COALESCE(_queue_id, 0) THEN
                        -- If a connection that was assigned a queue_id got put into the pool where it is not being used, maybe
                        -- throwing an exception might close this connection and bring the other one back into active service.
                        RAISE EXCEPTION 'Existing PID count % is at the queue size %.', _queue_id, _claim_queue_count
                        USING HINT = 'Close down connections that have a PID allocated and are not currently processing data.';
                END IF;
        END IF;

        SELECT  id_current.queue_id + 1
        INTO    _queue_id
        FROM    (       SELECT  id_current_sub.queue_id
                        FROM    $$||_out_table_queue_pid_ident||$$ AS id_current_sub
                        UNION ALL
                        SELECT  UNNEST(ARRAY[-1])
                ) AS id_current
        WHERE   NOT EXISTS (
                        SELECT
                        FROM    $$||_out_table_queue_pid_ident||$$ AS id_next
                        WHERE   id_next.queue_id = id_current.queue_id + 1)
        ORDER BY 1
        LIMIT   1;

        INSERT INTO $$||_out_table_queue_pid_ident||$$
        VALUES (_queue_id);

        RETURN _queue_id;
END;
$DEFINITION$;
$$;
        EXECUTE _sql_buffer;

        _function_name_ident    := quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_queue_pid_health');
        _sql_buffer := $$
CREATE OR REPLACE FUNCTION $$||quote_ident(out_table_def.schema::text) || '.' || _function_name_ident||$$(
        check_assignment_size   boolean DEFAULT true,
        check_pid_used_age      boolean DEFAULT true)
        RETURNS TABLE(result text)
        LANGUAGE plpgsql
        COST 20
        AS $DEFINITION$
DECLARE
        _queue_pid_count        bigint;
        _claim_queue_count      smallint;
        _claim_fixed_by_key     boolean;
        _queue_pid_used_age     interval;
        _used_age               interval;
BEGIN
        SELECT  claim_queue_count,      claim_fixed_by_key,     queue_pid_used_age
        INTO    _claim_queue_count,     _claim_fixed_by_key,    _queue_pid_used_age
        FROM    sys_syn.out_tables_def
        WHERE   in_table_id     = $$||quote_literal(out_table_def.in_table_id)||$$ AND
                out_group_id    = $$||quote_literal(out_table_def.out_group_id)||$$;

        IF check_assignment_size THEN
                IF _claim_queue_count IS NULL THEN
                        RAISE EXCEPTION 'claim_queue_count cannot be NULL.'
                        USING HINT =
                      'Health data on automatic queue_id assignment is not valid when automatic queue_id assignment is not in use.';
                END IF;

                SELECT  COUNT(*)
                INTO    _queue_pid_count
                FROM    $$||_out_table_queue_pid_ident||$$;

                IF _claim_queue_count != COALESCE(_queue_pid_count, 0) THEN
                        RETURN QUERY SELECT 'assignment_does_not_match_queue_size'::TEXT;
                        RETURN;
                END IF;
        END IF;

        IF check_pid_used_age THEN
                IF _queue_pid_used_age IS NULL THEN
                        RAISE EXCEPTION 'queue_pid_used_age cannot be NULL.'
                        USING HINT = 'Health data on maximum used age requires an interval for the maximum used age allowable.';
                END IF;

                IF _claim_fixed_by_key OR _claim_queue_count IS NOT NULL THEN
                        SELECT  clock_timestamp() - MIN(queue_pid.last_used)
                        INTO    _used_age
                        FROM    $$||_out_table_queue_pid_ident||$$ AS queue_pid;
                ELSE
                        SELECT  clock_timestamp() - MAX(queue_pid.last_used)
                        INTO    _used_age
                        FROM    $$||_out_table_queue_pid_ident||$$ AS queue_pid;
                END IF;

                IF COALESCE(_used_age, '0 seconds'::interval) > _queue_pid_used_age THEN
                        RETURN QUERY SELECT 'used_age_exceeded'::TEXT;
                        RETURN;
                END IF;
        END IF;

        RETURN QUERY SELECT 'good'::TEXT;
END;
$DEFINITION$;
$$;
        EXECUTE _sql_buffer;

        IF out_table_def.data_view THEN
                PERFORM sys_syn.util_out_table_view(out_table_def);
        END IF;
END;
$_$;
ALTER FUNCTION sys_syn.util_out_table_code(out_table_def sys_syn.out_tables_def) OWNER TO postgres;

CREATE FUNCTION sys_syn.util_out_table_exists_code(out_table_def sys_syn.out_tables_def, code_indent smallint) RETURNS text
        LANGUAGE plpgsql
        AS $_$
DECLARE
        _sql_buffer TEXT;
        _indent_sql TEXT;
        --_in_table_def sys_syn.in_tables_def;
        _out_table_baseline_ident TEXT;
        _out_table_locked_ident TEXT;
        _out_table_orphaned_ident TEXT;
        _out_table_queue_ident TEXT;
        _out_table_temp_ident TEXT;
        _out_table_log_ident TEXT;
BEGIN
        /*_in_table_def := (
                SELECT  in_tables_def
                FROM    sys_syn.in_tables_def
                WHERE   in_tables_def.in_table_id = _out_table_def.in_table_id);*/

        _indent_sql := repeat(E'        ', code_indent);

        _out_table_baseline_ident       := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_baseline');
        _out_table_locked_ident := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_locked');
        _out_table_orphaned_ident       := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_orphaned');
        _out_table_queue_ident  := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_queue');
        _out_table_temp_ident   := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_temp');
        _out_table_log_ident   := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_log');

        _sql_buffer := $$ AND
$$ || _indent_sql || $$NOT EXISTS (
$$ || _indent_sql || $$        SELECT
$$ || _indent_sql || $$        FROM    $$||_out_table_baseline_ident||$$ AS out_baseline
$$ || _indent_sql || $$        WHERE   out_baseline.key                = in_data.key AND
$$ || _indent_sql || $$                out_baseline.trans_id_in        = in_data.trans_id_in
$$ || _indent_sql || $$) AND
$$ || _indent_sql || $$NOT EXISTS (
$$ || _indent_sql || $$        SELECT
$$ || _indent_sql || $$        FROM    $$||_out_table_locked_ident||$$ AS out_locked
$$ || _indent_sql || $$        WHERE   out_locked.key          = in_data.key AND
$$ || _indent_sql || $$                out_locked.trans_id_in  = in_data.trans_id_in
$$ || _indent_sql || $$) AND
$$ || _indent_sql || $$NOT EXISTS (
$$ || _indent_sql || $$        SELECT
$$ || _indent_sql || $$        FROM    $$||_out_table_orphaned_ident||$$ AS out_orphaned
$$ || _indent_sql || $$        WHERE   out_orphaned.key                = in_data.key AND
$$ || _indent_sql || $$                out_orphaned.trans_id_in        = in_data.trans_id_in
$$ || _indent_sql || $$) AND
$$ || _indent_sql || $$NOT EXISTS (
$$ || _indent_sql || $$        SELECT
$$ || _indent_sql || $$        FROM    $$||_out_table_queue_ident||$$ AS out_queue
$$ || _indent_sql || $$        WHERE   out_queue.key           = in_data.key AND
$$ || _indent_sql || $$                out_queue.trans_id_in   = in_data.trans_id_in
$$ || _indent_sql || $$) AND
$$ || _indent_sql || $$NOT EXISTS (
$$ || _indent_sql || $$        SELECT
$$ || _indent_sql || $$        FROM    $$||_out_table_temp_ident||$$ AS out_temp
$$ || _indent_sql || $$        WHERE   out_temp.key            = in_data.key AND
$$ || _indent_sql || $$                out_temp.trans_id_in    = in_data.trans_id_in
$$ || _indent_sql || $$) AND
$$ || _indent_sql || $$NOT EXISTS (
$$ || _indent_sql || $$        SELECT
$$ || _indent_sql || $$        FROM    $$||_out_table_log_ident||$$ AS out_log
$$ || _indent_sql || $$        WHERE   out_log.key            = in_data.key AND
$$ || _indent_sql || $$                out_log.trans_id_in    = in_data.trans_id_in
$$ || _indent_sql || $$)$$;

        RETURN _sql_buffer;
END;
$_$;
ALTER FUNCTION sys_syn.util_out_table_exists_code(out_table_def sys_syn.out_tables_def, code_indent smallint)
        OWNER TO postgres;

CREATE FUNCTION sys_syn.util_out_tables_exists_code(in_table_def sys_syn.in_tables_def, code_indent smallint) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
        _sql_buffer TEXT;
        _out_table_def sys_syn.out_tables_def;
BEGIN
        _sql_buffer := '';

        FOR     _out_table_def IN
        SELECT  out_tables_def.*
        FROM    sys_syn.out_tables_def
        WHERE   out_tables_def.in_table_id = in_table_def.in_table_id
        ORDER BY out_tables_def.out_group_id
        LOOP

                _sql_buffer := _sql_buffer || sys_syn.util_out_table_exists_code(_out_table_def, code_indent);

        END LOOP;

        RETURN _sql_buffer;
END;
$$;
ALTER FUNCTION sys_syn.util_out_tables_exists_code(in_table_def sys_syn.in_tables_def, code_indent smallint)
        OWNER TO postgres;

CREATE FUNCTION sys_syn.util_out_tables_orphaned_code(out_table_def sys_syn.out_tables_def) RETURNS text
    LANGUAGE plpgsql
    AS $_$
DECLARE
        _sql_buffer                     TEXT;
        _foreign_key_loop               RECORD;
        _foreign_column_loop            sys_syn.in_foreign_keys;
        _add_key_delimiter              BOOLEAN;
        _add_column_delimiter           BOOLEAN;
        _out_table_orphaned_ident       TEXT;
        _out_table_temp_ident           TEXT;
        _in_table_ident                 TEXT;
        _foreign_column_table_ident     TEXT;
        _foreign_data_table_needed      BOOLEAN;
        _foreign_data_table_join        TEXT;
        _foreign_column_column_type     sys_syn.in_column_type;
        _primary_column_table_ident     TEXT;
        _primary_data_table_join        TEXT;
        _primary_in_table_ident         TEXT;
        _primary_column_column_type     sys_syn.in_column_type;
BEGIN
        _sql_buffer                     := '';
        _add_key_delimiter              := FALSE;
        _foreign_data_table_needed      := FALSE;

        FOR     _foreign_key_loop IN
        SELECT  in_foreign_keys.primary_table_id,       in_foreign_keys.foreign_key_index,
                bool_or(
                        sys_syn.util_column_name_to_in_column_type(
                                in_foreign_keys.primary_table_id,
                                in_foreign_keys.primary_column_name)
                        != 'Key'::sys_syn.in_column_type) AS primary_data_table_needed
        FROM    sys_syn.in_foreign_keys JOIN
                sys_syn.out_tables_def ON
                        out_tables_def.in_table_id = in_foreign_keys.primary_table_id
        WHERE   in_foreign_keys.foreign_table_id        = out_table_def.in_table_id AND
                out_tables_def.out_group_id             = out_table_def.out_group_id
        GROUP BY primary_table_id,                      in_foreign_keys.foreign_key_index
        LOOP

                IF _add_key_delimiter THEN
                        _sql_buffer := _sql_buffer || $$ OR
                $$;
                ELSE
                        _add_key_delimiter := TRUE;
                END IF;

                IF _foreign_key_loop.primary_data_table_needed THEN

                        _primary_in_table_ident := (
                                SELECT  quote_ident(in_tables_def.schema::text) || '.' ||
                                        quote_ident(in_tables_def.in_table_id||'_in')
                                FROM    sys_syn.in_tables_def
                                WHERE   in_tables_def.in_table_id = _foreign_key_loop.primary_table_id
                        );

                        _primary_data_table_join := $$ JOIN
                                        $$ || _primary_in_table_ident || $$ AS in_primary_baseline ON
                                                in_primary_baseline.key = out_primary_baseline.key$$;

                ELSE
                        _primary_data_table_join := '';
                END IF;

                _sql_buffer := _sql_buffer || $$NOT EXISTS (
                        SELECT
                        FROM    $$ || (
                                SELECT  quote_ident(schema::text) || '.' ||
                                        quote_ident(_foreign_key_loop.primary_table_id || '_' ||
                                        out_table_def.out_group_id || '_baseline')
                                FROM    sys_syn.out_tables_def AS primary_out_def
                                WHERE   primary_out_def.in_table_id     = _foreign_key_loop.primary_table_id AND
                                        primary_out_def.out_group_id    = out_table_def.out_group_id) ||
                        $$ AS out_primary_baseline$$ || _primary_data_table_join || $$
                        WHERE   $$;

                _add_column_delimiter := FALSE;

                FOR     _foreign_column_loop IN
                SELECT  in_foreign_keys.* -- Why is .* needed for this loop and not needed in similar loops?
                FROM    sys_syn.in_foreign_keys
                WHERE   in_foreign_keys.primary_table_id        = _foreign_key_loop.primary_table_id AND
                        in_foreign_keys.foreign_table_id        = out_table_def.in_table_id AND
                        in_foreign_keys.foreign_key_index       = _foreign_key_loop.foreign_key_index
                LOOP

                        IF _add_column_delimiter THEN
                                _sql_buffer := _sql_buffer || $$ AND
                                $$;
                        ELSE
                                _add_column_delimiter := TRUE;
                        END IF;

                        _primary_column_column_type := sys_syn.util_column_name_to_in_column_type(
                                _foreign_column_loop.primary_table_id, _foreign_column_loop.primary_column_name);

                        IF _primary_column_column_type = 'Key'::sys_syn.in_column_type THEN
                                _primary_column_table_ident     := 'out_primary_baseline';
                        ELSE
                                _primary_column_table_ident     := 'in_primary_baseline';
                        END IF;
                        _foreign_column_table_ident := 'out_temp';

                        _foreign_column_column_type := sys_syn.util_column_name_to_in_column_type(
                                _foreign_column_loop.foreign_table_id, _foreign_column_loop.foreign_column_name);

                        IF _foreign_column_column_type = 'Key'::sys_syn.in_column_type THEN
                                _foreign_column_table_ident     := 'out_temp';
                        ELSE
                                _foreign_column_table_ident     := 'in_temp';
                                _foreign_data_table_needed      := TRUE;
                        END IF;

                        _sql_buffer := _sql_buffer || '(' || quote_ident(_primary_column_table_ident) || '.' || quote_ident(
                                sys_syn.util_in_column_type_to_column_name(_primary_column_column_type)) ||
                                ').' || quote_ident(_foreign_column_loop.primary_column_name) || ' = ('
                                || quote_ident(_foreign_column_table_ident) || '.' || quote_ident(
                                sys_syn.util_in_column_type_to_column_name(_foreign_column_column_type)) ||
                                ').' || quote_ident(_foreign_column_loop.foreign_column_name);

                END LOOP;

                _sql_buffer := _sql_buffer || $$
                )$$;

        END LOOP;

        IF _sql_buffer IS NULL THEN
                RAISE EXCEPTION
                        'There was a null value in one of the variables in the sys_syn.util_out_tables_orphaned_code function.'
                        USING HINT = 'Check the .* comment or add COALESCE to the variables to debug.';
        END IF;

        IF _sql_buffer = '' THEN
                RETURN '';
        END IF;

        _out_table_orphaned_ident       := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_orphaned');
        _out_table_temp_ident           := quote_ident(out_table_def.schema::text) || '.' ||
                quote_ident(out_table_def.in_table_id || '_' || out_table_def.out_group_id||'_temp');

        IF _foreign_data_table_needed THEN
                _in_table_ident                 := (
                        SELECT  quote_ident(in_tables_def.schema::text) || '.' ||
                                quote_ident(in_tables_def.in_table_id||'_in')
                        FROM    sys_syn.in_tables_def
                        WHERE   in_tables_def.in_table_id = out_table_def.in_table_id
                );

                _foreign_data_table_join := $$ JOIN
                        $$ || _in_table_ident || $$ AS in_temp ON
                                in_temp.trans_id_in     = out_temp.trans_id_in AND
                                in_temp.key             = out_temp.key$$;
        ELSE
                _foreign_data_table_join := '';
        END IF;

        _sql_buffer := $$-- Keep orphaned records out of the queue for now.

        INSERT INTO $$||_out_table_orphaned_ident||$$
        SELECT  out_temp.*
        FROM    $$||_out_table_temp_ident||$$ AS out_temp$$ || _foreign_data_table_join || $$
        WHERE   $$||_sql_buffer||$$
        ON CONFLICT (key) DO UPDATE
        SET     trans_id_in = EXCLUDED.trans_id_in;

        DELETE FROM $$||_out_table_temp_ident||$$ AS out_temp
        WHERE   EXISTS (
                        SELECT
                        FROM    $$||_out_table_orphaned_ident||$$ AS out_orphaned
                        WHERE   out_orphaned.key = out_temp.key
                );

        $$;

        RETURN _sql_buffer;
END;
$_$;
ALTER FUNCTION sys_syn.util_out_tables_orphaned_code(out_table_def sys_syn.out_tables_def) OWNER TO postgres;

CREATE OR REPLACE FUNCTION sys_syn.pre_pull_add_sql(
        relation                        regclass,
        in_group_id                     text,
        schema                          regnamespace DEFAULT NULL,
        in_pull_id                      text DEFAULT NULL,
        full_prepull_id                 text DEFAULT NULL,
        changes_prepull_id              text DEFAULT NULL,
        in_table_id                     text DEFAULT NULL,
        key_columns                     name[] DEFAULT NULL::name[],
        no_diff_columns                 name[] DEFAULT NULL::name[],
        omit_columns                    name[] DEFAULT NULL::name[],
        limit_to_columns                name[] DEFAULT NULL::name[]
)
        RETURNS text
        LANGUAGE plpgsql COST 10
        AS $_$
DECLARE
        _full_prepull_id        TEXT := pre_pull_add_sql.full_prepull_id;
        _changes_prepull_id     TEXT := pre_pull_add_sql.changes_prepull_id;
        _in_table_id            TEXT := pre_pull_add_sql.in_table_id;
        _name_schema            TEXT := pre_pull_add_sql.schema;
        _name_relation          TEXT;
        _name_unlogged_full     TEXT;
        _name_unlogged_changes  TEXT;
        _name_function_full     TEXT;
        _name_function_changes  TEXT;
        _sql_unlogged_table_def TEXT := NULL;
        _sql_unlogged_insert    TEXT;
        _sql_unlogged_select    TEXT;
        _column                 pg_catalog.pg_attribute%ROWTYPE;
        _column_name            TEXT;
        _format_type            TEXT;
        _sql_key_columns        TEXT;
        _return                 TEXT;
BEGIN
        IF _name_schema IS NULL THEN
                _name_schema := (       SELECT  pg_namespace.nspname
                                        FROM    pg_catalog.pg_namespace JOIN
                                                pg_catalog.pg_class ON
                                                        pg_class.relnamespace = pg_namespace.oid
                                        WHERE   pg_class.oid = pre_pull_add_sql.relation::oid);
        END IF;

        _name_relation := (     SELECT  pg_class.relname
                                FROM    pg_class
                                WHERE   pg_class.oid = pre_pull_add_sql.relation::oid);

        IF _full_prepull_id IS NULL THEN
                _full_prepull_id := _name_relation;
        END IF;

        IF _in_table_id IS NULL THEN
                _in_table_id := _full_prepull_id;
        END IF;

        IF key_columns IS NULL THEN
                _sql_key_columns := (
                        SELECT  'ARRAY[' || array_to_string(array_agg(quote_literal(attname) ORDER BY pg_attribute.attnum), ',') ||
                                                        ']'
                        FROM    pg_catalog.pg_attribute JOIN pg_catalog.pg_index ON
                                        pg_index.indrelid = pg_attribute.attrelid AND
                                        pg_index.indisprimary
                        WHERE   pg_attribute.attrelid = pre_pull_add_sql.relation AND
                                pg_attribute.attnum > 0 AND
                                NOT pg_attribute.attisdropped AND
                                pg_attribute.attnum = ANY(pg_index.indkey)
                );
        ELSE
                _sql_key_columns := 'ARRAY[' || array_to_string(array_agg(quote_literal(key_columns), ',')) || ']';
        END IF;

        _name_unlogged_full := quote_ident(_name_schema) || '.' || quote_ident(_full_prepull_id || '_prepull_full');
        _name_unlogged_changes := quote_ident(_name_schema) || '.' || quote_ident(_changes_prepull_id || '_prepull_changes');
        _name_function_full := quote_ident(_name_schema) || '.' || quote_ident(_full_prepull_id || '_prepull_full');
        _name_function_changes := quote_ident(_name_schema) || '.' || quote_ident(_changes_prepull_id || '_prepull_changes');

        FOR     _column IN
        SELECT  *
        FROM    pg_catalog.pg_attribute
        WHERE   pg_attribute.attrelid = pre_pull_add_sql.relation AND
                pg_attribute.attnum > 0 AND
                NOT pg_attribute.attisdropped AND
                (pre_pull_add_sql.limit_to_columns IS NULL OR pg_attribute.attname  = ANY(pre_pull_add_sql.limit_to_columns)) AND
                (pre_pull_add_sql.omit_columns IS NULL     OR pg_attribute.attname != ANY(pre_pull_add_sql.omit_columns))
        ORDER BY pg_attribute.attnum
        LOOP

                _column_name            := _column.attname;
                _format_type            := format_type(_column.atttypid, _column.atttypmod);

                IF _sql_unlogged_table_def IS NULL THEN
                        _sql_unlogged_table_def := '        trans_id_in sys_syn.trans_id DEFAULT sys_syn.trans_id_get() NOT NULL,
        ';
                        _sql_unlogged_insert := 'trans_id_in,
                ';
                        _sql_unlogged_select := 'sys_syn.trans_id_get(),
                ';
                ELSE
                        _sql_unlogged_table_def := _sql_unlogged_table_def || ',
        ';
                        _sql_unlogged_insert := _sql_unlogged_insert || ',
                ';
                        _sql_unlogged_select := _sql_unlogged_select || ',
                ';
                END IF;

                _sql_unlogged_table_def := _sql_unlogged_table_def || quote_ident(_column_name) || E'\t' || _format_type;
                _sql_unlogged_insert := _sql_unlogged_insert || quote_ident(_column_name);
                _sql_unlogged_select := _sql_unlogged_select || quote_ident(_column_name);

        END LOOP;

        _return := $$CREATE UNLOGGED TABLE $$ || _name_unlogged_full || $$ (
$$ || _sql_unlogged_table_def || $$
);
$$;
        IF _name_unlogged_changes IS NOT NULL THEN
                _return := _return || $$CREATE UNLOGGED TABLE $$ || _name_unlogged_changes || $$ (
$$ || _sql_unlogged_table_def || $$
);
$$;
        END IF;

        _return := _return || $$INSERT INTO sys_syn.prepulls_def(
        prepull_id,     schema)
VALUES ($$ || quote_literal(_full_prepull_id) || $$,   $$ || quote_literal(_name_schema) || $$);
$$;
        IF _changes_prepull_id IS NOT NULL THEN
                _return := _return || $$INSERT INTO sys_syn.prepulls_def(
        prepull_id,     schema)
VALUES ($$ || quote_literal(_changes_prepull_id) || $$,   $$ || quote_literal(_name_schema) || $$);
$$;
        END IF;
        _return := _return || $$CREATE OR REPLACE FUNCTION $$ || _name_function_full || $$()
        RETURNS boolean AS
$BODY$
DECLARE
        _prepull_def            sys_syn.prepulls_def%ROWTYPE;
        _in_pull_state          sys_syn.in_pulls_state%ROWTYPE;
        _possible_changes       BOOLEAN = FALSE;
BEGIN
        _prepull_def := (
                SELECT  prepulls_def
                FROM    sys_syn.prepulls_def
                WHERE   prepulls_def.prepull_id = $$ || quote_literal(_full_prepull_id) || $$);

        IF NOT pg_try_advisory_lock('sys_syn.prepulls_def'::regclass::int, _prepull_def.lock_id) THEN
                RAISE NOTICE 'Prepull % is running.', _prepull_def.prepull_id;
                RETURN FALSE;
        END IF;

        TRUNCATE $$ || _name_unlogged_full || $$;

        PERFORM sys_syn.in_trans_start(FALSE, ARRAY[]::TEXT[], TRUE);

        INSERT INTO $$ || _name_unlogged_full || $$ (
                $$ || _sql_unlogged_insert || $$)
        SELECT  $$ || _sql_unlogged_select || $$
        FROM    $$ || ( SELECT  quote_ident(pg_namespace.nspname) || '.' || quote_ident(pg_class.relname)
                        FROM    pg_catalog.pg_namespace JOIN
                                pg_catalog.pg_class ON
                                        pg_class.relnamespace = pg_namespace.oid
                        WHERE   pg_class.oid = pre_pull_add_sql.relation::oid) || $$;
        IF FOUND THEN _possible_changes = TRUE; END IF;

        PERFORM sys_syn.in_trans_finish();

        -- Remove changes that are older than the full dataset.
        -- DELETE FROM $$ || quote_ident(_full_prepull_id || '_prepull_changes') || $$ WHERE trans_id_in < sys_syn.trans_id_get();

        PERFORM pg_advisory_unlock('sys_syn.prepulls_def'::regclass::int, _prepull_def.lock_id);

        RETURN _possible_changes;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 2000;
SELECT sys_syn.in_table_add_sql(
        relation        => '$$ || _name_unlogged_full || $$'::regclass,
        in_group_id     => $$ || quote_literal(in_group_id) || $$,
        schema          => $$ || quote_literal(_name_schema) || $$,
        key_columns     => $$ || _sql_key_columns || $$,
        no_diff_columns => NULL,
        omit_columns    => ARRAY[]::TEXT[],
        limit_to_columns=> NULL,
        full_prepull_id => $$ || quote_literal(_full_prepull_id) || $$,
        changes_prepull_id=> $$ || quote_nullable(_changes_prepull_id) || $$,
        in_table_id     => $$ || quote_literal(_in_table_id) || $$);
$$;

        RETURN _return;
END;
$_$;
ALTER FUNCTION sys_syn.pre_pull_add_sql(
        relation                regclass,
        in_group_id             text,
        schema                  regnamespace,
        in_pull_id              text,
        full_prepull_id         text,
        changes_prepull_id      text,
        in_table_id             text,
        key_columns             name[],
        no_diff_columns         name[],
        omit_columns            name[],
        limit_to_columns        name[])
        OWNER TO postgres;

CREATE FUNCTION sys_syn.in_pull_sequence_populate_assume(in_group_id text default null) RETURNS void
        LANGUAGE plpgsql COST 20
        AS $_$
BEGIN
        -- Assume in_group_id is in_pull_sequence_id.

        INSERT INTO sys_syn.in_pull_sequences_def (
                in_pull_sequence_id)
        SELECT  in_tables_def.in_group_id
        FROM    sys_syn.in_tables_def
        WHERE   (in_tables_def.in_group_id = in_pull_sequence_populate_assume.in_group_id OR
                        in_pull_sequence_populate_assume.in_group_id IS NULL)
        GROUP BY in_tables_def.in_group_id
        ON CONFLICT DO NOTHING;

        INSERT INTO sys_syn.in_pull_sequence_pulls (
                in_pull_sequence_id,            sequence_index,         in_pull_id)
        SELECT  in_tables_def.in_group_id,      in_pulls_def.lock_id,   in_pulls_def.in_pull_id
        FROM    sys_syn.in_pulls_def JOIN sys_syn.in_tables_def USING (in_pull_id)
        WHERE   (in_tables_def.in_group_id = in_pull_sequence_populate_assume.in_group_id OR
                        in_pull_sequence_populate_assume.in_group_id IS NULL)
        ON CONFLICT DO NOTHING;
END;
$_$;
ALTER FUNCTION sys_syn.in_pull_sequence_populate_assume(in_group_id text) OWNER TO postgres;

CREATE FUNCTION sys_syn.jobs_get_crontab (
        in_pull_sequence_id     text DEFAULT NULL,
        pull_minute             text DEFAULT '6'::text,
        pull_hour               text DEFAULT '2'::text,
        pull_day_of_month       text DEFAULT '*'::text,
        pull_month              text DEFAULT '*'::text,
        pull_day_of_week        text DEFAULT '*'::text,
        processed_minute        text DEFAULT '*'::text,
        processed_hour          text DEFAULT '*'::text,
        processed_day_of_month  text DEFAULT '*'::text,
        processed_months        text DEFAULT '*'::text,
        processed_day_of_week   text DEFAULT '*'::text
        )
        RETURNS text
        LANGUAGE plpgsql COST 10
        AS $_$
DECLARE
        _return text;
        _psql   text;
BEGIN
        IF in_pull_sequence_id IS NULL THEN
                SELECT  array_to_string(array_agg(sys_syn.jobs_get_crontab(in_pull_sequence_pulls.in_pull_sequence_id)), '
')
                INTO STRICT _return
                FROM    sys_syn.in_pull_sequence_pulls;

                RETURN _return;
        END IF;

        SELECT  array_to_string(array_agg('BEGIN;SELECT ' || quote_ident(prepulls_def.schema::text) || '.' ||
                        quote_ident(prepulls_def.prepull_id || '_prepull_full') || '();COMMIT'
                        ORDER BY in_pull_sequence_pulls.sequence_index, in_tables_def.in_pull_order), ';') || ';'
        INTO    _psql
        FROM    sys_syn.in_pull_sequence_pulls JOIN
                sys_syn.in_tables_def ON
                        in_tables_def.in_pull_id = in_pull_sequence_pulls.in_pull_id JOIN
                sys_syn.prepulls_def ON
                        prepulls_def.prepull_id = in_tables_def.full_prepull_id
        WHERE   in_tables_def.full_prepull_id IS NOT NULL AND
                in_pull_sequence_pulls.in_pull_sequence_id = jobs_get_crontab.in_pull_sequence_id;

        SELECT  COALESCE(_psql, '') || array_to_string(array_agg('BEGIN;SELECT ' || quote_ident(in_pulls_def.schema::text) || '.' ||
                        quote_ident(in_pulls_def.in_pull_id || '_vacuum') || '();COMMIT'
                        ORDER BY in_pull_sequence_pulls.sequence_index), ';') || ';'
        INTO STRICT _psql
        FROM    sys_syn.in_pull_sequence_pulls JOIN
                sys_syn.in_pulls_def USING (in_pull_id)
        WHERE   in_pull_sequence_pulls.in_pull_sequence_id = jobs_get_crontab.in_pull_sequence_id
        GROUP BY in_pull_sequence_pulls.in_pull_id;

        SELECT  _psql || array_to_string(array_agg('BEGIN;SELECT ' || quote_ident(in_pulls_def.schema::text) || '.' ||
                        quote_ident(in_pulls_def.in_pull_id || '_pull') || '(FALSE);COMMIT'
                        ORDER BY in_pull_sequence_pulls.sequence_index), ';') || ';'
        INTO STRICT _psql
        FROM    sys_syn.in_pull_sequence_pulls JOIN
                sys_syn.in_pulls_def USING (in_pull_id)
        WHERE   in_pull_sequence_pulls.in_pull_sequence_id = jobs_get_crontab.in_pull_sequence_id
        GROUP BY in_pull_sequence_pulls.in_pull_id;

        SELECT  _psql || array_to_string(array_agg('BEGIN;SELECT ' || quote_ident(in_tables_def.schema::text) || '.' ||
                        quote_ident(in_tables_def.in_table_id || '_' || out_tables_def.out_group_id || '_move') || '();COMMIT'
                        ORDER BY in_pull_sequence_pulls.sequence_index, in_tables_def.in_pull_order), ';')
        INTO    _psql
        FROM    sys_syn.in_pull_sequence_pulls JOIN
                sys_syn.in_tables_def ON
                        in_tables_def.in_pull_id = in_pull_sequence_pulls.in_pull_id JOIN
                sys_syn.out_tables_def ON
                        out_tables_def.in_table_id = in_tables_def.in_table_id
        WHERE   in_pull_sequence_pulls.in_pull_sequence_id = jobs_get_crontab.in_pull_sequence_id;

        _return := ' '||pull_minute||'     '||pull_hour||'     '||pull_day_of_month||'     '||pull_month||'     '||
                pull_day_of_week||$$     psql '$$ || current_database() ||
                $$' -c 'BEGIN;SELECT sys_syn.distribute_load(60);COMMIT;$$ || _psql || $$' > /dev/shm/sys_syn-$$ ||
                in_pull_sequence_id || $$-pull.log 2>&1$$;

        SELECT  array_to_string(array_agg('BEGIN;SELECT ' || quote_ident(in_tables_def.schema::text) || '.' ||
                        quote_ident(in_tables_def.in_table_id || '_' || out_tables_def.out_group_id || '_processed') || '();COMMIT'
                        ORDER BY in_pull_sequence_pulls.sequence_index, in_tables_def.in_pull_order, out_tables_def.lock_id), ';')
        INTO    _psql
        FROM    sys_syn.in_pull_sequence_pulls JOIN
                sys_syn.in_tables_def ON
                        in_tables_def.in_pull_id = in_pull_sequence_pulls.in_pull_id JOIN
                sys_syn.out_tables_def ON
                        out_tables_def.in_table_id = in_tables_def.in_table_id
        WHERE   in_pull_sequence_pulls.in_pull_sequence_id = jobs_get_crontab.in_pull_sequence_id;

        _return := _return || '
 '||processed_minute||'     '||processed_hour||'     '||processed_day_of_month||'     '||processed_months||'     '||
                processed_day_of_week||$$     psql '$$ || current_database() ||
                $$' -c 'BEGIN;SELECT sys_syn.distribute_load(60);COMMIT;$$ || _psql || $$' > /dev/shm/sys_syn-$$ ||
                in_pull_sequence_id || $$-processed.log 2>&1$$;

        RETURN _return;
END;
$_$;
ALTER FUNCTION sys_syn.jobs_get_crontab(
        in_pull_sequence_id     text,
        pull_minute             text,
        pull_hour               text,
        pull_day_of_month       text,
        pull_month              text,
        pull_day_of_week        text,
        processed_minute        text,
        processed_hour          text,
        processed_day_of_month  text,
        processed_months        text,
        processed_day_of_week   text) OWNER TO postgres;

CREATE FUNCTION sys_syn.jobs_get_pgagent (
        in_pull_sequence_id     text DEFAULT NULL,
        delete_sql              boolean DEFAULT FALSE,
        create_sql              boolean DEFAULT TRUE,
        job_steps_only          boolean DEFAULT FALSE,
        pull_minute             boolean[] DEFAULT '{f,f,f,t,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,
                f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f}'::boolean[],
        pull_hour               boolean[] DEFAULT '{f,f,f,t,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f}'::boolean[],
        pull_day_of_month       boolean[] DEFAULT '{t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t}'::boolean[],
        pull_month              boolean[] DEFAULT '{t,t,t,t,t,t,t,t,t,t,t,t}'::boolean[],
        pull_day_of_week        boolean[] DEFAULT '{t,t,t,t,t,t,t}'::boolean[],
        processed_minute        boolean[] DEFAULT '{t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,
                t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t}'::boolean[],
        processed_hour          boolean[] DEFAULT '{t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t}'::boolean[],
        processed_day_of_month  boolean[] DEFAULT '{t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t}'::boolean[],
        processed_months        boolean[] DEFAULT '{t,t,t,t,t,t,t,t,t,t,t,t}'::boolean[],
        processed_day_of_week   boolean[] DEFAULT '{t,t,t,t,t,t,t}'::boolean[]
        )
        RETURNS text
        LANGUAGE plpgsql COST 10
        AS $_$
DECLARE
        _return                 text;
        _sql_job_id_in          text;
        _sql_job_in             text := '';
        _sql_delete             text := '';
        _sql_schedule_in        text := '';
        _sql_jobstep_in_delay   text := '';
        _sql_jobstep_prepull    text := '';
        _sql_jobstep_vacuum     text := '';
        _sql_jobstep_pull       text := '';
        _sql_jobstep_move       text := '';
        _sql_job_id_out         text;
        _sql_job_out            text := '';
        _sql_jobstep_out_delay  text := '';
        _sql_jobstep_processed  text := '';
        _sql_schedule_out       text := '';
BEGIN
        IF delete_sql = FALSE AND create_sql = FALSE THEN
                RAISE EXCEPTION 'delete_sql and/or create_sql must be true.'
                USING HINT = 'This function cannot return nothing.';
        END IF;

        IF in_pull_sequence_id IS NULL THEN
                SELECT  array_to_string(array_agg(sys_syn.jobs_get_pgagent(
                                in_pull_sequence_pulls.in_pull_sequence_id,
                                jobs_get_pgagent.delete_sql,
                                jobs_get_pgagent.create_sql,
                                jobs_get_pgagent.job_steps_only,
                                jobs_get_pgagent.pull_minute,
                                jobs_get_pgagent.pull_hour,
                                jobs_get_pgagent.pull_day_of_month,
                                jobs_get_pgagent.pull_month,
                                jobs_get_pgagent.pull_day_of_week,
                                jobs_get_pgagent.processed_minute,
                                jobs_get_pgagent.processed_hour,
                                jobs_get_pgagent.processed_day_of_month,
                                jobs_get_pgagent.processed_months,
                                jobs_get_pgagent.processed_day_of_week
                        )), '
')
                INTO STRICT _return
                FROM    sys_syn.in_pull_sequence_pulls;

                RETURN _return;
        END IF;

        _sql_job_id_in := 'sys_syn Pull Sequence ' || in_pull_sequence_id;

        IF create_sql AND NOT job_steps_only THEN
                _sql_job_in := $$

INSERT INTO pgagent.pga_job (
        jobjclid,       jobname) VALUES (
        2,              $$ || quote_literal(_sql_job_id_in) || $$);$$;
        END IF;

        _sql_job_id_in := $$(SELECT pga_job.jobid FROM pgagent.pga_job WHERE pga_job.jobname = $$ || quote_literal(_sql_job_id_in)
                || $$)$$;

        IF create_sql AND NOT job_steps_only THEN
                _sql_schedule_in := $$

INSERT INTO pgagent.pga_schedule (
        jscjobid,
        jscname,        jscdesc,        jscenabled,
        jscminutes,
        jschours,
        jscweekdays,
        jscmonthdays,
        jscmonths)
VALUES ($$ || _sql_job_id_in || $$,
        'Every Night',  '',             true,
        $$ || quote_literal(pull_minute) || $$,
        $$ || quote_literal(pull_hour) || $$,
        $$ || quote_literal(pull_day_of_week) || $$,
        $$ || quote_literal(pull_day_of_month) || $$,
        $$ || quote_literal(pull_month) || $$);
        $$;
        END IF;

        IF create_sql THEN
                _sql_jobstep_in_delay := $$

INSERT INTO pgagent.pga_jobstep (
        jstjobid,
        jstname,
        jstdesc,        jstenabled,     jstkind,
        jstcode,
        jstconnstr,     jstdbname,      jstonerror)
VALUES ($$ || _sql_job_id_in || $$,
        '1-distribute_load',
        '',             true,           's',
        'SELECT sys_syn.distribute_load(60)',
        '',             $$ || quote_literal(current_database()) || $$,      'f');$$;

                SELECT  array_to_string(array_agg($$

INSERT INTO pgagent.pga_jobstep (
        jstjobid,
        jstname,
        jstdesc,        jstenabled,     jstkind,
        jstcode,
        jstconnstr,     jstdbname,      jstonerror)
VALUES ($$ || _sql_job_id_in || $$,
        $$ || quote_literal($$2-prepull $$ || to_char(in_pull_sequence_pulls.sequence_index, 'FM00000') || '-' ||
                in_pull_sequence_pulls.in_pull_id || ' ' || to_char(in_tables_def.in_pull_order, 'FM00000') || '-' ||
                in_tables_def.in_table_id) || $$,
        '',             true,           's',
        'SELECT $$ || quote_ident(prepulls_def.schema::text) || $$.$$ || quote_ident(prepulls_def.prepull_id || $$_prepull_full$$)
                || $$()',
        '',             $$ || quote_literal(current_database()) || $$,      'f');$$
                                ORDER BY in_pull_sequence_pulls.sequence_index, in_tables_def.in_pull_order), ';')
                INTO    _sql_jobstep_prepull
                FROM    sys_syn.in_pull_sequence_pulls JOIN
                        sys_syn.in_tables_def ON
                                in_tables_def.in_pull_id = in_pull_sequence_pulls.in_pull_id JOIN
                        sys_syn.prepulls_def ON
                                prepulls_def.prepull_id = in_tables_def.full_prepull_id
                WHERE   in_tables_def.full_prepull_id IS NOT NULL AND
                        in_pull_sequence_pulls.in_pull_sequence_id = jobs_get_pgagent.in_pull_sequence_id;

                SELECT  array_to_string(array_agg($$

INSERT INTO pgagent.pga_jobstep (
        jstjobid,
        jstname,
        jstdesc,        jstenabled,     jstkind,
        jstcode,
        jstconnstr,     jstdbname,      jstonerror)
VALUES ($$ || _sql_job_id_in || $$,
        $$ || quote_literal($$3-vacuum $$ || to_char(in_pull_sequence_pulls.sequence_index, 'FM00000') || '-' ||
                in_pull_sequence_pulls.in_pull_id) || $$,
        '',             true,           's',
        'SELECT $$ || quote_ident(in_pulls_def.schema::text) || $$.$$ || quote_ident(in_pulls_def.in_pull_id || $$_vacuum$$) ||
                $$()',
        '',             $$ || quote_literal(current_database()) || $$,      'f');$$
                                ORDER BY in_pull_sequence_pulls.sequence_index), ';')
                INTO STRICT _sql_jobstep_vacuum
                FROM    sys_syn.in_pull_sequence_pulls JOIN
                        sys_syn.in_pulls_def USING (in_pull_id)
                WHERE   in_pull_sequence_pulls.in_pull_sequence_id = jobs_get_pgagent.in_pull_sequence_id
                GROUP BY in_pull_sequence_pulls.in_pull_id;

                SELECT  array_to_string(array_agg($$

INSERT INTO pgagent.pga_jobstep (
        jstjobid,
        jstname,
        jstdesc,        jstenabled,     jstkind,
        jstcode,
        jstconnstr,     jstdbname,      jstonerror)
VALUES ($$ || _sql_job_id_in || $$,
        $$ || quote_literal($$4-pull $$ || to_char(in_pull_sequence_pulls.sequence_index, 'FM00000') || '-' ||
                in_pull_sequence_pulls.in_pull_id) || $$,
        '',             true,           's',
        'SELECT $$ || quote_ident(in_pulls_def.schema::text) || $$.$$ || quote_ident(in_pulls_def.in_pull_id || '_pull') ||
                $$(FALSE)',
        '',             $$ || quote_literal(current_database()) || $$,      'f');$$
                        ORDER BY in_pull_sequence_pulls.sequence_index), ';')
                INTO STRICT _sql_jobstep_pull
                FROM    sys_syn.in_pull_sequence_pulls JOIN
                        sys_syn.in_pulls_def USING (in_pull_id)
                WHERE   in_pull_sequence_pulls.in_pull_sequence_id = jobs_get_pgagent.in_pull_sequence_id
                GROUP BY in_pull_sequence_pulls.in_pull_id;

                SELECT  array_to_string(array_agg($$

INSERT INTO pgagent.pga_jobstep (
        jstjobid,
        jstname,
        jstdesc,        jstenabled,     jstkind,
        jstcode,
        jstconnstr,     jstdbname,      jstonerror)
VALUES ($$ || _sql_job_id_in || $$,
        $$ || quote_literal('5-move ' ||
                to_char(in_pull_sequence_pulls.sequence_index, 'FM00000')       || '-' || in_pull_sequence_pulls.in_pull_id ||' '||
                to_char(out_tables_def.lock_id, 'FM00000')                      || '-' || out_tables_def.out_group_id) || $$,
        '',             true,           's',
        'SELECT $$ || quote_ident(in_tables_def.schema::text) || $$.$$ ||
                quote_ident(in_tables_def.in_table_id || '_' || out_tables_def.out_group_id || '_move') || $$()',
        '',             $$ || quote_literal(current_database()) || $$,      'f');$$
                        ORDER BY in_pull_sequence_pulls.sequence_index, in_tables_def.in_pull_order, out_tables_def.lock_id), ';')
                INTO STRICT _sql_jobstep_move
                FROM    sys_syn.in_pull_sequence_pulls JOIN
                        sys_syn.in_tables_def ON
                                in_tables_def.in_pull_id = in_pull_sequence_pulls.in_pull_id JOIN
                        sys_syn.out_tables_def ON
                                out_tables_def.in_table_id = in_tables_def.in_table_id
                WHERE   in_pull_sequence_pulls.in_pull_sequence_id = jobs_get_pgagent.in_pull_sequence_id;
        END IF;

        _sql_job_id_out := 'sys_syn Processed Sequence ' || in_pull_sequence_id;

        IF create_sql AND NOT job_steps_only THEN
                _sql_job_out := $$

INSERT INTO pgagent.pga_job (
        jobjclid,       jobname) VALUES (
        3,              $$ || quote_literal(_sql_job_id_out) || $$);$$;
        END IF;

        _sql_job_id_out := $$(SELECT pga_job.jobid FROM pgagent.pga_job WHERE pga_job.jobname = $$ ||
                quote_literal(_sql_job_id_out) || $$)$$;

        IF create_sql AND NOT job_steps_only THEN
                _sql_schedule_out := $$

INSERT INTO pgagent.pga_schedule (
        jscjobid,
        jscname,        jscdesc,        jscenabled,
        jscminutes,
        jschours,
        jscweekdays,
        jscmonthdays,
        jscmonths)
VALUES ($$ || _sql_job_id_out || $$,
        'Every Minute', '',             true,
        $$ || quote_literal(processed_minute) || $$,
        $$ || quote_literal(processed_hour) || $$,
        $$ || quote_literal(processed_day_of_week) || $$,
        $$ || quote_literal(processed_day_of_month) || $$,
        $$ || quote_literal(processed_months) || $$);
        $$;
        END IF;

        IF create_sql THEN
                _sql_jobstep_out_delay := $$

INSERT INTO pgagent.pga_jobstep (
        jstjobid,
        jstname,
        jstdesc,        jstenabled,     jstkind,
        jstcode,
        jstconnstr,     jstdbname,      jstonerror)
VALUES ($$ || _sql_job_id_out || $$,
        '1-distribute_load',
        '',             true,           's',
        'SELECT sys_syn.distribute_load(60)',
        '',             $$ || quote_literal(current_database()) || $$,      'f');$$;

                SELECT  array_to_string(array_agg($$

INSERT INTO pgagent.pga_jobstep (
        jstjobid,
        jstname,
        jstdesc,        jstenabled,     jstkind,
        jstcode,
        jstconnstr,     jstdbname,      jstonerror)
VALUES ($$ || _sql_job_id_out || $$,
        $$ || quote_literal('2-processed ' ||
                to_char(in_pull_sequence_pulls.sequence_index,  'FM00000') || '-' || in_pull_sequence_pulls.in_pull_id  || ' ' ||
                to_char(in_tables_def.in_pull_order,            'FM00000') || '-' || in_tables_def.in_pull_id           || ' ' ||
                to_char(out_tables_def.lock_id,                 'FM00000') || '-' || out_tables_def.out_group_id)       || $$,
        '',             true,           's',
        'SELECT $$ || quote_ident(in_tables_def.schema::text) || $$.$$ ||
                quote_ident(in_tables_def.in_table_id || '_' || out_tables_def.out_group_id || '_processed') || $$()',
        '',             $$ || quote_literal(current_database()) || $$,      'f');$$
                          ORDER BY in_pull_sequence_pulls.sequence_index, in_tables_def.in_pull_order, out_tables_def.lock_id), ';')
                INTO STRICT _sql_jobstep_processed
                FROM    sys_syn.in_pull_sequence_pulls JOIN
                        sys_syn.in_tables_def ON
                                in_tables_def.in_pull_id = in_pull_sequence_pulls.in_pull_id JOIN
                        sys_syn.out_tables_def ON
                                out_tables_def.in_table_id = in_tables_def.in_table_id
                WHERE   in_pull_sequence_pulls.in_pull_sequence_id = jobs_get_pgagent.in_pull_sequence_id;
        END IF;

        IF delete_sql THEN
                _sql_delete := $$

DELETE FROM pgagent.pga_jobsteplog
USING   pgagent.pga_jobstep
WHERE   pga_jobsteplog.jsljstid = pga_jobstep.jstid AND
        (pga_jobstep.jstjobid = $$ || _sql_job_id_in || $$ OR
        pga_jobstep.jstjobid = $$ || _sql_job_id_out || $$);

DELETE FROM pgagent.pga_jobstep
WHERE   jstjobid = $$ || _sql_job_id_in || $$ OR
        jstjobid = $$ || _sql_job_id_out || $$;$$;

                IF NOT job_steps_only THEN
                        _sql_delete := _sql_delete || $$

DELETE FROM pgagent.pga_joblog
WHERE   jlgjobid = $$ || _sql_job_id_in || $$ OR
        jlgjobid = $$ || _sql_job_id_out || $$;

DELETE FROM pgagent.pga_schedule
WHERE   jscjobid = $$ || _sql_job_id_in || $$ OR
        jscjobid = $$ || _sql_job_id_out || $$;

DELETE FROM pgagent.pga_job
WHERE   jobid = $$ || _sql_job_id_in || $$ OR
        jobid = $$ || _sql_job_id_out || $$;$$;
                END IF;
        END IF;

        RETURN  $$BEGIN;$$ ||
                COALESCE(_sql_delete, '') ||
                COALESCE(_sql_job_in, '') ||
                COALESCE(_sql_schedule_in, '') ||
                COALESCE(_sql_jobstep_in_delay, '') ||
                COALESCE(_sql_jobstep_prepull, '') ||
                COALESCE(_sql_jobstep_vacuum, '') ||
                COALESCE(_sql_jobstep_pull, '') ||
                COALESCE(_sql_jobstep_move, '') ||
                COALESCE(_sql_job_out, '') ||
                COALESCE(_sql_schedule_out, '') ||
                COALESCE(_sql_jobstep_out_delay, '') ||
                COALESCE(_sql_jobstep_processed, '') || $$

COMMIT;$$;
END;
$_$;
ALTER FUNCTION sys_syn.jobs_get_pgagent(
        in_pull_sequence_id     text,
        delete_sql              boolean,
        create_sql              boolean,
        job_steps_only          boolean,
        pull_minute             boolean[],
        pull_hour               boolean[],
        pull_day_of_month       boolean[],
        pull_month              boolean[],
        pull_day_of_week        boolean[],
        processed_minute        boolean[],
        processed_hour          boolean[],
        processed_day_of_month  boolean[],
        processed_months        boolean[],
        processed_day_of_week   boolean[])
        OWNER TO postgres;


CREATE VIEW sys_syn.in_foreign_keys_view AS
SELECT  in_foreign_keys.primary_table_id,
        in_foreign_keys.foreign_table_id,
        in_foreign_keys.foreign_key_index,
        sys_syn.util_column_name_to_in_column_type(in_foreign_keys.primary_table_id, (in_foreign_keys.primary_column_name)::name)
                AS primary_column_in_column_type,
        in_foreign_keys.primary_column_name,
        sys_syn.util_column_name_to_data_type(in_foreign_keys.primary_table_id, (in_foreign_keys.primary_column_name)::name)
                AS primary_column_data_type,
        sys_syn.util_column_name_to_in_column_type(in_foreign_keys.foreign_table_id, (in_foreign_keys.foreign_column_name)::name)
                AS foreign_column_in_column_type,
        in_foreign_keys.foreign_column_name,
        sys_syn.util_column_name_to_data_type(in_foreign_keys.foreign_table_id, (in_foreign_keys.foreign_column_name)::name)
                AS foreign_column_data_type
FROM    sys_syn.in_foreign_keys;
ALTER TABLE sys_syn.in_foreign_keys_view OWNER TO postgres;

CREATE VIEW sys_syn.in_table_columns_view AS
SELECT  in_table_columns_def.in_table_id,
        in_table_columns_def.column_index,
        in_table_columns_def.column_name,
        sys_syn.util_column_name_to_in_column_type(in_table_columns_def.in_table_id, in_table_columns_def.column_name)
                AS in_column_type,
        sys_syn.util_column_name_to_data_type(in_table_columns_def.in_table_id, in_table_columns_def.column_name) AS data_type,
        in_table_columns_def.source_in_expression
FROM    sys_syn.in_table_columns_def;
ALTER TABLE sys_syn.in_table_columns_view OWNER TO postgres;

CREATE VIEW sys_syn.out_queue_data_view_columns_view AS
SELECT  out_tables_def.in_table_id,
        out_tables_def.out_group_id,
        pg_attribute.attname,
        format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
FROM    sys_syn.out_tables_def JOIN
        --pg_namespace ON
        --      pg_namespace.oid = out_tables_def.schema JOIN
        pg_class ON
                pg_class.relnamespace = out_tables_def.schema AND
                pg_class.relname = out_tables_def.in_table_id||'_'||out_tables_def.out_group_id||'_queue_data' JOIN
        pg_attribute ON
                pg_attribute.attrelid = pg_class.oid AND
                pg_attribute.attnum > 0 AND
                NOT pg_attribute.attisdropped
WHERE   out_tables_def.data_view = TRUE
ORDER BY pg_attribute.attnum;
ALTER TABLE sys_syn.out_queue_data_view_columns_view OWNER TO postgres;

/*
CREATE VIEW sys_syn.foreign_keys_view AS
SELECT  parent_namespace.nspname AS parent_schema,
        parent_class.relname AS parent_table,
        child_namespace.nspname AS child_schema,
        child_class.relname AS child_table,
        foreign_ids.constraint_name,
        foreign_ids.key_column_index,
        parent_attribute.attname AS parent_column,
        child_attribute.attname AS child_column
FROM    (
                SELECT  child_constraint.confrelid,
                        child_constraint.conrelid,
                        child_constraint.conname AS constraint_name,
                        array_position(child_constraint.conkey, unnest(child_constraint.conkey)) AS key_column_index,
                        unnest(child_constraint.confkey) AS parent_attnum,
                        unnest(child_constraint.conkey) AS child_attnum
                FROM    pg_class AS child_class JOIN
                        pg_constraint AS child_constraint ON
                                child_constraint.conrelid = child_class.oid
                WHERE   child_constraint.contype = 'f'
        ) AS foreign_ids JOIN
        pg_attribute AS parent_attribute ON
                parent_attribute.attrelid = foreign_ids.confrelid AND
                parent_attribute.attnum = foreign_ids.parent_attnum AND
                parent_attribute.attnum > 0 AND
                NOT parent_attribute.attisdropped JOIN
        pg_class AS parent_class ON
                parent_class.oid = foreign_ids.confrelid JOIN
        pg_class AS child_class ON
                child_class.oid = foreign_ids.conrelid JOIN
        pg_attribute AS child_attribute ON
                child_attribute.attrelid = foreign_ids.conrelid AND
                child_attribute.attnum = foreign_ids.child_attnum AND
                child_attribute.attnum > 0 AND
                NOT child_attribute.attisdropped JOIN
        pg_namespace AS child_namespace ON
                child_class.relnamespace = child_namespace.oid JOIN
        pg_namespace AS parent_namespace ON
                parent_class.relnamespace = parent_namespace.oid
ORDER BY 1, 2, 3, 4, 5, 6;
ALTER TABLE sys_syn.foreign_keys_view OWNER TO postgres;
*/

SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_column_transforms', $$WHERE rule_group_id NOT LIKE 'sys_syn-%'$$);
SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_foreign_keys', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_groups_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_pull_sequence_pulls', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_pull_sequences_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_pulls_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_pulls_request', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_pulls_state', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_table_columns_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_tables_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.in_trans_log', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.out_column_transforms', $$WHERE rule_group_id NOT LIKE 'sys_syn-%'$$);
SELECT pg_catalog.pg_extension_config_dump('sys_syn.out_groups_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.out_tables_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.out_tables_state', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.out_view_columns_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.prepulls_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn.settings', '');
