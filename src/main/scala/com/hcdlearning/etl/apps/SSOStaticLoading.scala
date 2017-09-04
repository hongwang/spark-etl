package com.hcdlearning.etl.apps

import com.hcdlearning.etl.common.SparkSupported
import com.hcdlearning.etl.common.execution.{ ExecuteContext, ExecuteEngine }
import com.hcdlearning.etl.common.definitions.Recipe
import com.hcdlearning.etl.common.definitions.steps._

object SSOStaticLoading extends App with SparkSupported {

  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

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

    val steps: List[BaseStep] = new CassandraInputStep(
        "read_from_source", 
        "sso", 
        "application", 
        coalesce = Some(1), 
        registerTo = "reg_raw_application") :: 
      new ParquetOutputStep("save_staging", "Overwrite", "hdfs://nameservice-01/user/datahub/staging/sso/{workflow_id}/{name}_raw_data") ::
      new SQLOutputStep("write_to_dw", loading_sql) ::
      Nil

    val topotaxy = Seq(
      "read_from_source" -> "save_staging",
      "save_staging" -> "write_to_dw"
    )

    val recipe = Recipe("sso-static-loading", steps, topotaxy)
    val ctx = ExecuteContext(spark, params)
    ExecuteEngine.run(ctx, recipe)
  }
}
