#! /bin/bash

set -eu

pg_config_dir=${PGDATA-}
pg_config_path=
pg_reload=1

function get_boolean() {
    case "$(echo "$1" | tr '[:upper:]' '[:lower:]')" in
        1|t|true|y|yes)
            echo 1
            return
        ;;
        0|f|false|n|no)
            echo 0
            return
        ;;
    esac
    echo "Value \"$1\" is not a recognized boolean value." >&2
    exit 2
}

parsed_args=$(getopt -o "d:c:r:" -l "datadirectory:,configfile:,reload:" -n "$(basename $0)" -- "$@")

if [ $? -ne 0 ]; then
    exit 1
fi

eval set -- "$parsed_args"

while [[ $# -gt 1 ]]; do
    case "$1" in
        -d|--datadirectory)
            pg_config_dir=$2
            shift
        ;;
        -c|--configfile)
            pg_config_path=$2
            shift
        ;;
        -r|--reload)
            pg_reload=$(get_boolean $2)
            shift
        ;;
        *)
            exit 2
        ;;
    esac
    shift
done

if [ -n "$pg_config_dir" ]; then
    if [ -z "$pg_config_path" ]; then
        if [ -e "$pg_config_dir/postgresql.conf" ]; then
            pg_config_path=$pg_config_dir/postgresql.conf
        fi
    fi
fi

superuser_reserved_connections=3
user_connections=20
sys_syn_connections=$(($(nproc) + 1))
sys_syn_dblink_connections=$(($(nproc) + 1))
max_connections=$(($sys_syn_connections + $sys_syn_dblink_connections + $superuser_reserved_connections + $user_connections))
tune_config_name=sys_syn_tune.conf
if [ -z "$pg_config_dir" ]; then
    pg_config_dir=$(psql -tAc 'show data_directory' postgres)
fi
if [ -z "$pg_config_path" ]; then
    pg_config_path=$(psql -tAc 'show config_file' postgres)
fi
tune_config_path=${pg_config_dir}/${tune_config_name}
tune_profile=dw

memory_free_kb=$(free -k | grep "buffers/cache" | awk '{print $4}')

# From https://github.com/le0pard/pgtune/blob/master/source/javascripts/pgtune.coffee
if [ $tune_profile = "dw" ]; then
    shared_buffers=$(($memory_free_kb / 4))
    effective_cache_size=$(($memory_free_kb * 3 / 4))
    work_mem=$(($shared_buffers / $max_connections / 2))
    maintenance_work_mem=$(($memory_free_kb / 8))
    if [ $maintenance_work_mem -gt 2097152 ]; then
        maintenance_work_mem=2097152
    fi
    min_wal_size=4096
    max_wal_size=8192
    checkpoint_completion_target=.9
    wal_buffers=$((3 * $shared_buffers / 100))
    if [ $wal_buffers -gt 16384 ]; then
        wal_buffers=16384
    fi
    default_statistics_target=500
else
    exit 2
fi

cat > "$tune_config_path" <<EOF
max_connections = ${max_connections}
shared_buffers = ${shared_buffers}kB
effective_cache_size = ${effective_cache_size}kB
work_mem = ${work_mem}kB
maintenance_work_mem = ${maintenance_work_mem}kB
min_wal_size = ${min_wal_size}MB
max_wal_size = ${max_wal_size}MB
checkpoint_completion_target = ${checkpoint_completion_target}
wal_buffers = ${wal_buffers}kB
default_statistics_target = ${default_statistics_target}
max_locks_per_transaction = 256
EOF

if ! grep -q "include.*=.*${tune_config_name}" "$pg_config_path"; then
    echo "include = '$tune_config_name'" >> "$pg_config_path"
fi

echo "Updated $tune_config_path"
if [ $pg_reload -ne 0 ]; then
    echo -n "PostgreSQL "; pg_ctl reload
    echo 'You may need to run "pg_ctl restart" to make all changes effective.'
fi
