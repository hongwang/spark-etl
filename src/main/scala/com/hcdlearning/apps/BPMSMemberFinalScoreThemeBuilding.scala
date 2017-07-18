package com.hcdlearning.apps

import com.hcdlearning.common.SparkSupported
import com.hcdlearning.common.execution.{ ExecuteContext, ExecuteEngine }
import com.hcdlearning.common.definitions.Recipe
import com.hcdlearning.common.definitions.steps._

object BPMSMemberFinalScoreThemeBuilding extends App with SparkSupported {

  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

    val steps: List[BaseStep] = new CSVInputStep(
      "load_csv",
      "hdfs://nameservice-01/user/datahub/staging/marksimos/{workflow_id}/theme_member_final_score.csv.gz",
      header=true,
      compression="gzip",
      registerTo="daily_member_final_score"
    ) :: Nil

    val topotaxy = Seq()

    val recipe = Recipe("bpms-member_final_score-theme-building", steps, topotaxy)
    val ctx = ExecuteContext(spark, params)
    ExecuteEngine.run(ctx, recipe)
  }
}