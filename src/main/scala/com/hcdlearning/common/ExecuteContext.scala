package com.hcdlearning.common

import org.apache.spark.sql.SparkSession

class ExecuteContext(spark: SparkSession) {
    def getSpark = spark
}