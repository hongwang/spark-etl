package com.hcdlearning.apps

import com.hcdlearning.common.SparkSupported
import com.hcdlearning.common.execution.{ ExecuteContext, ExecuteEngine }
import com.hcdlearning.common.definitions.Recipe
import com.hcdlearning.common.definitions.steps._

object SSODailyLoading extends App with SparkSupported {

  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

    val steps: List[BaseStep] = new CassandraInputStep(
        "read_member_from_hz", 
        "sso_archive_hz", 
        "member", 
        "archive_date = '{target_date}'",
        registerTo = "reg_raw_application") :: Nil

    val topotaxy = Seq("" -> "")

    val recipe = Recipe("sso-daily-loading", steps, topotaxy)
    val ctx = ExecuteContext(spark, params)
    ExecuteEngine.run(ctx, recipe)
  }

}
