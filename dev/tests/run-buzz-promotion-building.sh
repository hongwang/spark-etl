#!/usr/bin/env bash

BASEDIR=$(dirname $0)

SPARK_APP_NAME="spark-etl-buzz-promotion-building"
SPARK_APP_JAR="spark-etl-assembly-1.4.jar"
SPARK_APP_CLASS="com.hcdlearning.etl.apps.BuzzPromotionBuilding"
SPARK_APP_ARGS="--workflow_id xxxxx --target_date 2017-08-15"

${BASEDIR}/run.sh ${SPARK_APP_NAME} ${SPARK_APP_JAR} ${SPARK_APP_CLASS} ${SPARK_APP_ARGS}