#!/usr/bin/env bash

DINO_ENV=$1
N_WORKERS=$2
PORT=$3

ENVIRONMENT=${DINO_ENV} uvicorn --host 0.0.0.0 --port ${PORT} --workers=${N_WORKERS} server:app
