#!/usr/bin/env bash

NAMENODE=$1
FILE_PATH=$2

STORE_URL=$(curl -i -s -X GET "http://${NAMENODE}/webhdfs/v1${FILE_PATH}?op=OPEN" | grep Location | awk '{print $NF}' | sed 's/[[:cntrl:]]//')
curl -X GET ${STORE_URL}