package com.hcdlearning.sso

import com.hcdlearning.buzz.common.{ SparkSupported }
import com.hcdlearning.common.ExecuteEngine
import com.hcdlearning.common.steps.{ CassandraInputStep, ParquetOutputStep }

object SSOStaticETL extends SparkSupported {

  def main(args: Array[String]): Unit = {
    val cassandra_step = new CassandraInputStep("sso", "application")
    val parquet_step = new ParquetOutputStep("Overwrite", "hdfs://nameservice-01/user/datahub/staging/temp/etl_test")

    ExecuteEngine.run(spark, List(cassandra_step, parquet_step))
  }
}
