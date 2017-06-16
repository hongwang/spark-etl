package com.hcdlearning.common

import org.apache.spark.sql.SparkSession
import com.hcdlearning.common.definitions.steps.BaseStep

object ExecuteEngine extends Logging {

  def run(ctx: ExecuteContext, recipe: Recipe) = {
    logInfo(s"Recipe ${recipe.name} start to run")
    val start = System.nanoTime

    logInfo("Recipe ${recipe.name} has ${recipe.phases.length} phases")
    for(phase <- recipe.phases) {
      //TODO: run the tasks in parallel
      for(task <- phase.tasks) {
        task.steps.foreach(_ => _.run(ctx))
      }
    }


    logInfo("Recipe %d finished, took %f s".format(recipe.name, (System.nanoTime - start) / 1e9))
  }

}