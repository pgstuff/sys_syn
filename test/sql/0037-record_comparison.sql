BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id          integer NOT NULL,
        test_table_text         text,
        test_table_int          integer,
        test_table_other        text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');
END$$;

CREATE FUNCTION user_data.test_table_out_record_comparison(
        left_id                user_data.test_table_in_id,
        left_attributes         user_data.test_table_in_attributes,
        left_no_diff            user_data.test_table_in_no_diff,
        right_attributes        user_data.test_table_in_attributes,
        right_no_diff           user_data.test_table_in_no_diff--,
        --attributes_different    boolean
        )
        RETURNS boolean AS
$BODY$
DECLARE
        _different BOOLEAN;
BEGIN
        _different =
                (
                        -- Normally, you would want case changes to go through.  But if the text ends up in all upper or lower case,
                        -- the following comparison might be useful to reduce unnecessary deltas.
                        COALESCE(LOWER(left_attributes.test_table_text), '')  !=
                        COALESCE(LOWER(right_attributes.test_table_text), '')
                ) OR (
                        left_attributes.test_table_int IS DISTINCT FROM
                        right_attributes.test_table_int
                ) OR (
                        row(left_attributes.test_table_other)::record *<>
                        row(right_attributes.test_table_other)::record
                );

        -- For citext, you must cast both values to text in order to use the standard equality operations:
        --      'test'::citext::text IS DISTINCT FROM 'Test'::citext::text
        -- For other data types that can have different representations for the same equality, use internal binary representation:
        --      row('test'::citext)::record *<> row('Test'::citext)::record

        IF _different IS NULL THEN
                RAISE EXCEPTION
                        'The _different result in user_data.test_table_out_record_comparison is NULL when testing record %.',
                        left_id::text
                USING HINT = 'Make sure that the expression is using COALESCE, IS [NOT] DISTINCT FROM, or row()::record *=/*<>.';
        END IF;

        --IF attributes_different THEN
                RETURN _different;
        --ELSE
        --        RETURN NOT _different;
        --END IF;
END;
$BODY$
        LANGUAGE plpgsql VOLATILE
        COST 20;
ALTER FUNCTION user_data.test_table_out_record_comparison(
        left_id                user_data.test_table_in_id,
        left_attributes         user_data.test_table_in_attributes,
        left_no_diff            user_data.test_table_in_no_diff,
        right_attributes        user_data.test_table_in_attributes,
        right_no_diff           user_data.test_table_in_no_diff--,
        --attributes_different    boolean
        )
        OWNER TO postgres;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES (1,              'test_data v1');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out',
        --record_comparison_same          =>
        --        'NOT user_data.test_table_out_record_comparison(%1.id, %1.attributes, %1.no_diff, %2.attributes, %2.no_diff)',
        record_comparison_different     =>
                'user_data.test_table_out_record_comparison(%1.id, %1.attributes, %1.no_diff, %2.attributes, %2.no_diff)');

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT  test_table_out_queue.id
FROM    user_data.test_table_out_queue;

UPDATE user_data.test_table_out_queue SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
SELECT user_data.test_table_out_processed();

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline out_baseline
        LEFT JOIN user_data.test_table_in AS in_data USING (trans_id_in, id);

UPDATE user_data.test_table SET test_table_text = 'TEST_DATA v1' WHERE test_table_id = 1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 2;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT  test_table_out_queue.id
FROM    user_data.test_table_out_queue;

UPDATE user_data.test_table SET test_table_text = 'Test_Data v2' WHERE test_table_id = 1;

UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1;SET LOCAL sys_syn.trans_id_curr TO 3;
SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT  test_table_out_queue.id
FROM    user_data.test_table_out_queue;

UPDATE user_data.test_table_out_queue SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
UPDATE user_data.test_table_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_table_id = 1;
SELECT user_data.test_table_out_processed();

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    user_data.test_table_out_baseline out_baseline
        LEFT JOIN user_data.test_table_in AS in_data USING (trans_id_in, id);

ROLLBACK;
