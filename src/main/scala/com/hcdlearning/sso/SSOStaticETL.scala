package com.hcdlearning.sso

import com.hcdlearning.common.{ SparkSupported, ExecuteContext, ExecuteEngine }
import com.hcdlearning.common.definitions.steps._

object SSOStaticETL extends SparkSupported {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      throw new IllegalStateException("some argument must be specified.")
    }

    val (workflowId, inspect) = args match {
      case Array(workflowId, inspect) => (workflowId, inspect.toBoolean)
      case Array(workflowId) => (workflowId, false)
    }

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
      new ParquetOutputStep("save_staging", "Overwrite", "hdfs://nameservice-01/user/datahub/staging/sso/{workflowId}/{name}_raw_data") ::
      new SQLOutputStep("write_to_dw", loading_sql) ::
      Nil

    val recipe = Recipe("sso-static-loading", steps)
    val ctx = new ExecuteContext(spark, workflowId, inspect=inspect)
    ExecuteEngine.run(ctx, recipe)
  }
}
