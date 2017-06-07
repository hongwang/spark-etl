package com.hcdlearning.common

import org.apache.spark.sql.SparkSession
import com.hcdlearning.common.steps.BaseStep

object ExecuteEngine extends Logging {

  def run(ctx: ExecuteContext, steps: List[BaseStep]) = {
    logger.info(s"start to run ${steps.length} steps")

    steps.foreach(step => step.run(ctx))

    logger.info("finished run steps")
  }

}