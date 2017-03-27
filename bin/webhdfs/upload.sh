#!/usr/bin/env bash

HDFS_HOST=$1
FILE_PATH=$2
LOCAL_FILE_PATH=$3

STORE_URL=$(curl -i -s -X PUT "${HDFS_HOST}/webhdfs/v1${FILE_PATH}?op=CREATE&overwrite=true" 2>/dev/null | grep Location | awk '{print $NF}' | sed 's/[[:cntrl:]]//')
curl -i -X PUT -T ${LOCAL_FILE_PATH} ${STORE_URL}