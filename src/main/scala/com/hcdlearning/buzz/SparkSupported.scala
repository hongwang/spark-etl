package com.hcdlearning.buzz

import org.apache.spark.sql.SparkSession

trait SparkSupported {

  val spark: SparkSession = SparkSession
    .builder()
//    .appName("Spark sql on Hive")
//    .master("spark://uat-bigdata-01:6066,uat-bigdata-02:6066,uat-bigdata-03:6066")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

}
