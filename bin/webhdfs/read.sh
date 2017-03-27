#!/usr/bin/env bash

HDFS_HOST=$1
FILE_PATH=$2

STORE_URL=$(curl -i -s -X GET "${HDFS_HOST}/webhdfs/v1${FILE_PATH}?op=OPEN" | grep Location | awk '{print $NF}' | sed 's/[[:cntrl:]]//')
curl -X GET ${STORE_URL}