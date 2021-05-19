#!/usr/bin/env bash

if [ $# -lt 4 ]; then
    echo "usage: $0 <conda executable> <conda environment> <dino environment> <dino home>"
    exit 1
fi

LOG_DIR=/var/log/dino
CONDA_EXEC=$1
CONDA_ENV=$2
DINO_ENV=$3
DINO_HOME=$4

if [[ ! -d ${LOG_DIR} ]]; then
    if ! mkdir -p ${LOG_DIR}; then
        echo "error: could not create missing log directory '$LOG_DIR'"
        exit 1
    fi
fi

# functions are not exported to subshells so need to re-evaluate them from install path
source ~/.bashrc
eval "$(${CONDA_EXEC} shell.bash hook)"

if ! ${CONDA_EXEC} activate ${CONDA_ENV}; then
    echo "error: could not activate conda environment '$CONDA_ENV'"
    exit 1
fi

set -x

cd ${DINO_HOME} && DINO_HOME=${DINO_HOME} ENVIRONMENT=${DINO_ENV} python deleter.py
