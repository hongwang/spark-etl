package com.hcdlearning.common

import org.apache.spark.sql.SparkSession
import com.hcdlearning.common.steps.BaseStep

object ExecuteEngine extends Logging {

  def run(spark: SparkSession, steps: List[BaseStep]) = {
    logger.info(s"start to run ${steps.length} steps")

    val ctx = new ExecuteContext(spark)
    for(step <- steps) {
      step.execute(ctx)
    }

    logger.info("finished steps")
  }

}