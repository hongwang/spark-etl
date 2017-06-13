package com.hcdlearning.common

import org.apache.spark.sql.SparkSession

trait SparkSupported {

  val spark: SparkSession = SparkSession
    .builder()
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()
}
