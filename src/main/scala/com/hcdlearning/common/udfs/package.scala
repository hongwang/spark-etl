package com.hcdlearning.common

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.udf

package object udfs {

  // private val _UDFs = Map(
  //   "timestamp_from_uuid" -> udf(DateTimeUDFs.timestamp_from_uuid),
  //   "fake" -> udf(DateTimeUDFs.fake)
  // )

  object implicits {
    implicit class SparkWithUDFRegistor(val spark: SparkSession) {

      // cannot use this way before Spark 2.2.0, udf can be register directly in Spark 2.2.0
      // we will enable this after Spark 2.2.0 released
      // def register(name: String) {
      //   if (!_UDFs.contains(name)) throw new ETLException(s"cannot find udf: $name")

      //   spark.udf.register(name, _UDFs(name))
      // }

      def registerAll() {
        spark.udf.register("timestamp_from_uuid", DateTimeUDFs.timestamp_from_uuid)
        spark.udf.register("fake", DateTimeUDFs.fake)
      }
    }
  }
}