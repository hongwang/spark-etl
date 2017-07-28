#!/usr/bin/env bash

BASEDIR=$(dirname $0)

SPARK_APP_NAME="spark-etl-bpms-member_final_score-theme-building"
SPARK_APP_JAR="spark-etl-assembly-1.2.jar"
SPARK_APP_CLASS="com.hcdlearning.apps.BPMSMemberFinalScoreThemeBuilding"
SPARK_APP_ARGS="--staging_path hdfs://nameservice-01/user/datahub/staging/marksimos/ --workflow_id xxxxx --seminar_ids 1024,1025"

${BASEDIR}/run.sh ${SPARK_APP_NAME} ${SPARK_APP_JAR} ${SPARK_APP_CLASS} ${SPARK_APP_ARGS}