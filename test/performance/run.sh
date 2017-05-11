#! /bin/bash

set -eu

number_of_rows=$1
partition_count=$2
max_processes=$3

psql_options="-q"
generate_1=$(($number_of_rows / 4 + 4))

psql $psql_options -c 'DROP DATABASE IF EXISTS contrib_performance' postgres
psql $psql_options -c 'CREATE DATABASE contrib_performance' postgres

psql $psql_options -v ON_ERROR_STOP=1 contrib_performance <<EOF
CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data;

CREATE TABLE user_data.test_table (
        test_id_text text NOT NULL,
        test_id_int integer NOT NULL,
        test_attr_text_1 text,
        test_attr_text_2 text,
        test_attr_text_3 text,
        test_attr_int_1 integer,
        test_attr_int_2 integer,
        test_attr_bigint bigint,
        test_attr_date date,
        test_attr_int8range int8range,
        test_attr_timestamp timestamp with time zone,
        CONSTRAINT test_table_pkey PRIMARY KEY (test_id_text, test_id_int));

CREATE TABLE user_data.test_table_load (LIKE user_data.test_table);

INSERT INTO user_data.test_table_load
SELECT  left(md5('key'||ser_1), 10) AS test_id_text,
        ser_2 AS test_id_int,
        left(md5('data1'||ser_1||ser_2), 10 + ser_2) AS test_attr_text_1,
        left(md5('data2'||ser_1||ser_2), 4 + ser_2 * 2) AS test_attr_text_2,
        left(md5('data3'||ser_1||ser_2), 4 + (ser_2 % 2) * 4) AS test_attr_text_3,
        ser_1 * 4 + ser_2 - 4 AS test_attr_int_1,
        ser_2 + ser_1 % 10 AS test_attr_int_2,
        ser_1 * 50 + ser_2 * 20 + (ser_1 % 5) * 2 + ser_2 * 3 AS test_attr_bigint,
        (DATE '1980-01-01' + (INTERVAL '1 days' * ((ser_1 * 32 + ser_2 * 16 + (ser_1 % 4) * 2 + ser_2 * 8))))::DATE test_attr_date,
        int8range(ser_1 * 256, ser_1 * 256 + ser_2 * 4 + ser_1 + (ser_1 % 16) * 4) AS test_attr_int8range,
        (TIMESTAMPTZ '1980-01-08 00:12:34 -8:00' + (INTERVAL '1 days' * ((ser_1 * 40 + ser_2 * 16 + (ser_1 % 8) + ser_2 * 8)))) + (INTERVAL '1 seconds' * ser_1 * ser_2) + (INTERVAL '1 hours' * ser_2) AS test_attr_timestamp
FROM    generate_series(1, $generate_1) AS ser_1,
        generate_series(1,4) AS ser_2
LIMIT   $number_of_rows;

INSERT INTO sys_syn.in_groups_def (in_group_id, rule_group_ids) VALUES ('in', ARRAY['sys_syn-general']);

SELECT sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');

DO \$\$BEGIN
        EXECUTE sys_syn.in_table_create_sql(
                'user_data.test_table'::regclass,
                'in',
                in_partition_count => ${partition_count}::smallint);
END\$\$;

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out');
EOF

proc_seq=$(seq 1 $partition_count)

clock_load_start=$(date "+%s.%N")
psql $psql_options -v ON_ERROR_STOP=1 contrib_performance <<EOF
INSERT INTO user_data.test_table
SELECT  *
FROM    user_data.test_table_load;
EOF
clock_load_end=$(date "+%s.%N")

psql $psql_options -v ON_ERROR_STOP=1 contrib_performance <<EOF
DROP TABLE user_data.test_table_load;
EOF

clock_pull_full_start=$(date "+%s.%N")
psql $psql_options -v ON_ERROR_STOP=1 contrib_performance <<EOF
SELECT user_data.test_table_pull(FALSE);
EOF
clock_pull_full_end=$(date "+%s.%N")

clock_move_start=$clock_pull_full_end
echo "$proc_seq" | xargs -n 1 -I '{}' -P $max_processes psql $psql_options -v ON_ERROR_STOP=1 -c 'SELECT user_data.test_table_out_move_{}();' contrib_performance --
clock_move_end=$(date "+%s.%N")

clock_claim_start=$clock_move_end
echo "$proc_seq" | xargs -n 1 -I '{}' -P $max_processes psql $psql_options -v ON_ERROR_STOP=1 -c '
BEGIN;
SELECT sys_syn.in_trans_claim_start();
UPDATE user_data.test_table_out_queue_{} SET queue_state = '\''Claimed'\''::sys_syn.queue_state;
SELECT sys_syn.in_trans_finish();
COMMIT;' contrib_performance --
clock_claim_end=$(date "+%s.%N")

clock_process_start=$clock_claim_end
echo "$proc_seq" | xargs -n 1 -I '{}' -P $max_processes psql $psql_options -v ON_ERROR_STOP=1 -c '
BEGIN;
SELECT sys_syn.in_trans_claim_start();
UPDATE  user_data.test_table_out_queue_{}
SET     queue_state = '\''Processed'\''::sys_syn.queue_state
WHERE   queue_state = '\''Claimed'\''::sys_syn.queue_state;
SELECT sys_syn.in_trans_finish();
COMMIT;' contrib_performance --
clock_process_end=$(date "+%s.%N")

clock_processed_start=$clock_process_end
echo "$proc_seq" | xargs -n 1 -I '{}' -P $max_processes psql $psql_options -v ON_ERROR_STOP=1 -c 'SELECT user_data.test_table_out_processed_{}();' contrib_performance --
clock_processed_end=$(date "+%s.%N")

clock_vacuum_start=$clock_processed_end
echo "$proc_seq" | xargs -n 1 -I '{}' -P $max_processes psql $psql_options -v ON_ERROR_STOP=1 -c 'SELECT user_data.test_table_vacuum_{}();' contrib_performance --
clock_vacuum_end=$(date "+%s.%N")

time_load=$(echo "$clock_load_end - $clock_load_start" | bc)
time_pull_full=$(echo "$clock_pull_full_end - $clock_pull_full_start" | bc)
time_move=$(echo "$clock_move_end - $clock_move_start" | bc)
time_claim=$(echo "$clock_claim_end - $clock_claim_start" | bc)
time_process=$(echo "$clock_process_end - $clock_process_start" | bc)
time_processed=$(echo "$clock_processed_end - $clock_processed_start" | bc)
time_vacuum=$(echo "$clock_vacuum_end - $clock_vacuum_start" | bc)
time_total=$(echo "$time_pull_full + $time_move + $time_claim + $time_process + $time_processed + $time_vacuum" | bc)

function print_time () {
   printf "%10s" $(echo "scale=3; $1 / 1" | bc)
}

function print_load_per () {
   printf "%7s" $(echo "scale=0; 100 * $2 / $1" | bc)
}

echo "TASK            TIME  LOAD %"
echo "Load:     $(print_time $time_load) $(print_load_per $time_load $time_load)"
echo "Pull full:$(print_time $time_pull_full) $(print_load_per $time_load $time_pull_full)"
echo "Move:     $(print_time $time_move) $(print_load_per $time_load $time_move)"
echo "Claim:    $(print_time $time_claim) $(print_load_per $time_load $time_claim)"
echo "Process:  $(print_time $time_process) $(print_load_per $time_load $time_process)"
echo "Processed:$(print_time $time_processed) $(print_load_per $time_load $time_processed)"
echo "Vacuum:   $(print_time $time_vacuum) $(print_load_per $time_load $time_vacuum)"
echo "TOTAL:    $(print_time $time_total) $(print_load_per $time_load $time_total)"
echo
echo "sys_syn time multiplier: $(echo "scale=1; $time_total / $time_load" | bc)x"
