#!/usr/bin/env bash

if [ $# -lt 4 ]; then
    echo "usage: $0 <conda environment> <dino environment> <port> <number of workers>"
    exit 1
fi

LOG_DIR=/var/log/dino
CONDA_ENV=$1
DINO_ENV=$2
PORT=$3
N_WORKERS=$4

re_digits='^[0-9]+$'

if ! [[ ${PORT} =~ ${re_digits} ]] ; then
    echo "error: port '${PORT}' is not a number"
    exit 1
fi
if [[ ${PORT} -lt 1 || ${PORT} -gt 65536 ]]; then
    echo "error: port '${PORT}' not in range 1-65536"
    exit 1
fi

if ! [[ ${N_WORKERS} =~ ${re_digits} ]] ; then
    echo "error: number of workers '${N_WORKERS}' is not a number"
    exit 1
fi
if [[ ${N_WORKERS} -lt 1 || ${N_WORKERS} -gt 40 ]]; then
    echo "error: number of workers '${N_WORKERS}' not in range 1-40"
    exit 1
fi

if [[ ! -d ${LOG_DIR} ]]; then
    if ! mkdir -p ${LOG_DIR}; then
        echo "error: could not create missing log directory '$LOG_DIR'"
        exit 1
    fi
fi

# functions are not exported to subshells so need to re-evaluate them from install path
source ~/.bashrc
eval "$(conda shell.bash hook)"

if ! which conda >/dev/null; then
    echo "error: conda executable not found"
    exit 1
fi
if ! conda activate ${CONDA_ENV}; then
    echo "error: could not activate conda environment '$CONDA_ENV'"
    exit 1
fi

ENVIRONMENT=${DINO_ENV} uvicorn --host 0.0.0.0 --port ${PORT} --workers=${N_WORKERS} server:app
