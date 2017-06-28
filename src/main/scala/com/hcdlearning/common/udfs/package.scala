package com.hcdlearning.common

import org.apache.spark.sql.SparkSession
import com.hcdlearning.common.udfs.DateUDFs

package object udfs {

  private val UDFs = Map(
    "uuid_to_timestamp" -> DateUDFs.to_timestamp_from_uuid,
    "fake" -> DateUDFs.fake
  )

  object implicits {
    implicit class SparkWithUDFsRegistor(val spark: SparkSession) {
      def register(name: String) {
        if (!UDFs.contains(name)) throw new ETLException(s"cannot find udf: $name")

        spark.udf.register(name, UDFs(name))
      }
    }
  }
}