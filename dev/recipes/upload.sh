#!/usr/bin/env bash

FILE=$1

BASEDIR=$(dirname $0)/../..

NAMENODES="uat-bigdata-01:50070,uat-bigdata-02:50070"
NAMENODE=$(${BASEDIR}/bin/webhdfs/find_active_namenode.sh ${NAMENODES})

${BASEDIR}/bin/webhdfs/upload.sh "${NAMENODE}" "/user/spark/recipes/${FILE}" "${BASEDIR}/dev/recipes/${FILE}"