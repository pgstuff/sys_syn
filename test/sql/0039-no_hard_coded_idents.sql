BEGIN;

CREATE EXTENSION tinyint SCHEMA public;
CREATE EXTENSION sys_syn;

CREATE TABLE public.test_data (
        test_data_id integer NOT NULL,
        test_data_text text,
        CONSTRAINT test_data_pid PRIMARY KEY (test_data_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

SELECT sys_syn.in_table_create_sql('public.test_data'::regclass, 'in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('public.test_data'::regclass, 'in');
END$$;

INSERT INTO public.test_data(
        test_data_id, test_data_text)
VALUES (1,              'test_data v1');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('public', 'test_data', 'out');

SELECT public.test_data_pull(FALSE);
SELECT public.test_data_out_move_1();
UPDATE public.test_data_out_queue_1 SET queue_state = 'Claimed'::sys_syn.queue_state WHERE (id).test_data_id = 1;
UPDATE public.test_data_out_queue_1 SET queue_state = 'Processed'::sys_syn.queue_state WHERE (id).test_data_id = 1;
SELECT public.test_data_out_processed_1();

SELECT  out_baseline.id,
        (in_data.id).*,
        (in_data.attributes).*
FROM    public.test_data_out_baseline_1 out_baseline
        LEFT JOIN public.test_data_in_1 AS in_data USING (trans_id_in, id);

ROLLBACK;
