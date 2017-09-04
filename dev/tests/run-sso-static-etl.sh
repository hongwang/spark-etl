#!/usr/bin/env bash

BASEDIR=$(dirname $0)

SPARK_APP_NAME="spark-etl-sso-static-loading"
SPARK_APP_JAR="spark-etl-assembly-1.2.jar"
SPARK_APP_CLASS="com.hcdlearning.etl.apps.SSOStaticLoading"
SPARK_APP_ARGS=""

${BASEDIR}/run.sh ${SPARK_APP_NAME} ${SPARK_APP_JAR} ${SPARK_APP_CLASS} ${SPARK_APP_ARGS}