package com.hcdlearning.etl.common

import org.apache.spark.sql.SparkSession

trait SparkSupported {

  val spark: SparkSession = SparkSession
    .builder()
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    // .config("spark.cassandra.connection.host", "10.20.32.71,10.20.32.72,10.20.32.73")
    // .config("spark.cassandra.auth.username", "farseer")
    // .config("spark.cassandra.auth.password", "HCDhcd@123")
    .enableHiveSupport()
    .getOrCreate()
}
