#!/usr/bin/env bash

NAMENODE=$1
FILE_PATH=$2
SAVE_TO=$3

STORE_URL=$(curl -i -s -X GET "http://${NAMENODE}/webhdfs/v1${FILE_PATH}?op=OPEN" 2>/dev/null | grep Location | awk '{print $NF}' | sed 's/[[:cntrl:]]//')

if [ -d "${SAVE_TO}" ] ; then
    FILE_NAME=${STORE_URL%%\?*}
    FILE_NAME=${FILE_NAME##*/}
    SAVE_TO="${SAVE_TO}/${FILE_NAME}"
fi

wget -q "${STORE_URL}" -O ${SAVE_TO}

echo ${SAVE_TO}