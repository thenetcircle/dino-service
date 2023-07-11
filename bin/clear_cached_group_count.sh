#!/bin/bash

# needs to be run during deployment if previous version was <0.6.4

HOST=$1
PORT=$2
DB=$3

if [[ -z "$HOST" ]]; then
    echo "no host specified"
    exit 1
fi

if [[ -z "$PORT" ]]; then
    echo "no port specified"
    exit 1
fi

if [[ -z "$DB" ]]; then
    echo "no db specified"
    exit 1
fi

set -x

redis-cli -h ${HOST} -p ${PORT} -n ${DB} KEYS "group:count:inclhidden:*" | xargs --no-run-if-empty redis-cli -h ${HOST} -p ${PORT} -n ${DB} DEL
redis-cli -h ${HOST} -p ${PORT} -n ${DB} KEYS "group:count:visible:*"    | xargs --no-run-if-empty redis-cli -h ${HOST} -p ${PORT} -n ${DB} DEL
