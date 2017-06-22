package com.hcdlearning.recipes

import com.hcdlearning.common.SparkSupported
import com.hcdlearning.common.execution.{ ExecuteContext, ExecuteEngine }
import com.hcdlearning.common.definitions.Recipe
import com.hcdlearning.common.definitions.steps._

object SSODailyLoading extends SparkSupported {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      throw new IllegalStateException("some argument must be specified.")
    }

    val (workflowId, targetDateArg, inspect) = args match {
      case Array(workflowId, inspect) => (workflowId, inspect.toBoolean)
      case Array(workflowId, targetDate) => (workflowId, false)
    }
    
    val targetDate = parse(targetDateArg, `yyyy-MM-dd`)

    val steps: List[BaseStep] = new CassandraInputStep(
        "read_member_from_hz", 
        "sso_archive_hz", 
        "member", 
        s"archive_date = '${TARGET_DATE_FORMATTED}'",
        registerTo = "reg_raw_application") :: Nil

    val topotaxy = Seq("" -> "")

    // val recipe = Recipe("sso-daily-loading", steps, topotaxy)
    // val ctx = new ExecuteContext(spark, workflowId, inspect)
    // ExecuteEngine.run(ctx, recipe)
  }
}
