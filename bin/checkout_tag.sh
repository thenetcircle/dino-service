#!/usr/bin/env bash

TAG_NAME=$1

echo "fetching new tags from git... "
if ! git fetch; then
    echo "error: could not fetch from git"
    exit 1
fi

echo "switching to tag $TAG_NAME... "
if ! git checkout $TAG_NAME; then
    echo "error: could not switch to specified dino tag $TAG_NAME"
    exit 1
fi
