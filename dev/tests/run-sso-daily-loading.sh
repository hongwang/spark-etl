#!/usr/bin/env bash

BASEDIR=$(dirname $0)

SPARK_APP_NAME="spark-etl-sso-daily-loading"
SPARK_APP_JAR="spark-etl-assembly-1.2.jar"
SPARK_APP_CLASS="com.hcdlearning.apps.SSODailyLoading"
SPARK_APP_ARGS="--target_date 2017-06-15"

${BASEDIR}/run.sh ${SPARK_APP_NAME} ${SPARK_APP_JAR} ${SPARK_APP_CLASS} ${SPARK_APP_ARGS}