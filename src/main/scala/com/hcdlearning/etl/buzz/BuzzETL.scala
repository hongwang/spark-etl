package com.hcdlearning.etl.buzz

import com.hcdlearning.etl.common.{SparkSupported}
import com.hcdlearning.etl.buzz.common.DateFormat._
import com.hcdlearning.etl.buzz.common.{ETLContext}
import com.hcdlearning.etl.buzz.tasks._

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
