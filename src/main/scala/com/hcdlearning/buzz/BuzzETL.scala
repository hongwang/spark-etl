package com.hcdlearning.buzz

import com.hcdlearning.buzz.common.DateFormat._
import com.hcdlearning.buzz.common.{ETLContext, SparkSupported}
import com.hcdlearning.buzz.tasks._

object BuzzETL extends SparkSupported {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      throw new IllegalStateException("some argument must be specified.")
    }

    val Array(workflowId, targetDateArg) = args
    val targetDate = parse(targetDateArg, `yyyy-MM-dd`)

    val ctx = new ETLContext(workflowId, targetDate)

    QuizResultTask.run(spark, ctx)
    QuizResultGroupTask.run(spark, ctx)
    LessonTask.run(spark, ctx)
    QuizThemeBuildTask.run(spark, ctx)
  }
}
