package com.hcdlearning.buzz.tasks

import org.apache.spark.sql.SparkSession

import com.hcdlearning.buzz.common.ETLContext

object QuizThemeBuildTask {
  val name = "QuizResultTask"

  def run(spark: SparkSession, ctx: ETLContext) {

    import spark.implicits._

  }
}
