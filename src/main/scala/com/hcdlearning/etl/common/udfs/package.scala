package com.hcdlearning.etl.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

package object udfs {

  private val _UDFs = Map(
    "timestamp_from_uuid" -> udf(DateTimeUDFs.timestamp_from_uuid),
    "fake" -> udf(DateTimeUDFs.fake)
  )

  object implicits {
    implicit class SparkWithUDFRegistor(val spark: SparkSession) {

      def register(name: String) {
        if (!_UDFs.contains(name)) throw new ETLException(s"cannot find udf: $name")

        spark.udf.register(name, _UDFs(name))
      }
    }
  }
}