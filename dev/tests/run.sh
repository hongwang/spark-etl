#!/usr/bin/env bash

SPARK_APP_NAME=$1;shift
SPARK_APP_JAR=$1;shift
SPARK_APP_CLASS=$1;shift


BASEDIR=$(dirname $0)/../..

NAMENODES="uat-bigdata-01:50070,uat-bigdata-02:50070"
NAMENODE=$(${BASEDIR}/bin/webhdfs/find_active_namenode.sh ${NAMENODES})


SPARK_MASTER="spark://uat-bigdata-01:6066,uat-bigdata-02:6066,uat-bigdata-03:6066"
SPARK_APP_PATH="target/scala-2.11/${SPARK_APP_JAR}"
SPARK_APP_URL="hdfs://nameservice-01/user/spark/app/${SPARK_APP_JAR}"
SPARK_APP_ARGS="--workflow_id test_workflow --inspect true"

SPARK_ARGS="
    --class ${SPARK_APP_CLASS}
    --name ${SPARK_APP_NAME}
    --master ${SPARK_MASTER}
    --deploy-mode cluster
    --driver-cores 1
    --driver-memory 512M
    --total-executor-cores 1
    --executor-memory 512M
    --verbose
"


sbt assembly
[ $? != 0 ] && echo "compile failed" && exit 1

${BASEDIR}/bin/webhdfs/upload.sh ${NAMENODE} "/user/spark/app/${SPARK_APP_JAR}" "${SPARK_APP_PATH}"
[ $? != 0 ] && echo "upload jar failed" && exit 1

/opt/spark/bin/spark-submit ${SPARK_ARGS} ${SPARK_APP_URL} ${SPARK_APP_ARGS} "$@"