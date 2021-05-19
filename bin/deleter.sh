#!/usr/bin/env bash

if [ $# -lt 3 ]; then
    echo "usage: $0 <conda environment> <dino environment> <dino home>"
    exit 1
fi

LOG_DIR=/var/log/dino
CONDA_ENV=$1
DINO_ENV=$2
DINO_HOME=$3

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

set -x

cd ${DINO_HOME} && ENVIRONMENT=${DINO_ENV} python deleter.py
