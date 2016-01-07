BEGIN;

CREATE EXTENSION tinyint
    SCHEMA public;

CREATE EXTENSION sys_syn;

CREATE TABLE public.test_data (
        test_data_key integer NOT NULL,
        test_data_text text,
        CONSTRAINT test_data_pkey PRIMARY KEY (test_data_key));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_add_sql('public.test_data'::regclass, 'in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql('public.test_data'::regclass, 'in');
END$$;

INSERT INTO public.test_data(
        test_data_key, test_data_text)
VALUES (1,              'test_data v1');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('public', 'test_data', 'out');

SELECT public.test_data_pull(FALSE);
SELECT public.test_data_out_move();
UPDATE public.test_data_out_queue SET queue_state = 'Processed'::sys_syn.queue_state WHERE (key).test_data_key = 1;
SELECT public.test_data_out_processed();

SELECT  out_baseline.key,
        (in_data.key).*,
        (in_data.attributes).*
FROM    public.test_data_out_baseline out_baseline
        LEFT JOIN public.test_data_in AS in_data USING (trans_id_in, key);

ROLLBACK;
