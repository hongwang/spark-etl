package com.hcdlearning.common.execution

import org.apache.spark.sql.SparkSession
import com.hcdlearning.common.definitions.Recipe
import com.hcdlearning.common.definitions.steps.BaseStep

object ExecuteEngine extends Logging {

  def run(ctx: ExecuteContext, recipe: Recipe) = {
    logInfo(s"Recipe ${recipe.name} start to run")
    val start = System.nanoTime

    logInfo("Recipe ${recipe.name} has ${recipe.steps.length} steps")
    val steps = recipe.topologicalSteps

    steps.foreach(step => step.run(ctx))

    logInfo("Recipe %d finished, took %f s".format(recipe.name, (System.nanoTime - start) / 1e9))
  }

}