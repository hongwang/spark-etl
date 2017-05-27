package com.hcdlearning.sso

import com.hcdlearning.buzz.common.{ SparkSupported }
import com.hcdlearning.common.ExecuteEngine
import com.hcdlearning.common.steps._

object SSOStaticETL extends SparkSupported {

  def main(args: Array[String]): Unit = {

    val loading_sql = s"""
      |INSERT overwrite TABLE sso.raw_application
      |SELECT application_id,
      |  name,
      |  security_key,
      |  redirect_url,
      |  scopes,
      |  is_deleted,
      |  cast(to_unix_timestamp(insert_date) as timestamp) as insert_date,
      |  cast(to_unix_timestamp(update_date) as timestamp) as update_date,
      |  current_timestamp as __insert_time
      |FROM reg_raw_application
    """.stripMargin

    val steps: List[BaseStep] = new CassandraInputStep("sso", "application", registerTo = "reg_raw_application") :: 
      new ParquetOutputStep("Overwrite", "hdfs://nameservice-01/user/datahub/staging/temp/etl_test") ::
      new SQLTransStep(loading_sql) ::
      Nil

    ExecuteEngine.run(spark, steps)
  }
}
