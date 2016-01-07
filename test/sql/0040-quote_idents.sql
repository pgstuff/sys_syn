BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA "User Data"
    AUTHORIZATION postgres;

CREATE TABLE "User Data"."Test Table" (
        "Test Table Key" integer NOT NULL,
        "Test Table Text" text,
        CONSTRAINT "Test Table_pkey" PRIMARY KEY ("Test Table Key"));

INSERT INTO sys_syn.in_groups_def VALUES ('In Group');

SELECT sys_syn.in_table_add_sql('"User Data"."Test Table"'::regclass, 'In Group');

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql('"User Data"."Test Table"'::regclass, 'In Group');
END$$;

INSERT INTO "User Data"."Test Table"(
        "Test Table Key", "Test Table Text")
VALUES (1,              'test_data v1');

INSERT INTO sys_syn.out_groups_def VALUES ('Out Group');

SELECT sys_syn.out_table_add_sql('User Data', 'Test Table', 'Out Group');

DO $$BEGIN
        EXECUTE sys_syn.out_table_add_sql('User Data', 'Test Table', 'Out Group');
END$$;

SELECT "User Data"."Test Table_pull"(FALSE);
SELECT "User Data"."Test Table_Out Group_move"();
UPDATE "User Data"."Test Table_Out Group_queue" SET queue_state = 'Processed'::sys_syn.queue_state WHERE (key)."Test Table Key" = 1;
SELECT "User Data"."Test Table_Out Group_processed"();

SELECT  out_baseline.key,
        (in_data.key).*,
        (in_data.attributes).*
FROM    "User Data"."Test Table_Out Group_baseline" out_baseline
        LEFT JOIN "User Data"."Test Table_in" AS in_data USING (trans_id_in, key);

ROLLBACK;
