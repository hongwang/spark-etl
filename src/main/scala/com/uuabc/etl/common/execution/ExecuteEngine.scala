package com.uuabc.etl.common.execution

import com.uuabc.etl.common.Logging
import com.uuabc.etl.common.definitions.Recipe

object ExecuteEngine extends Logging {

  def run(ctx: ExecuteContext, recipe: Recipe) = {
    logInfo(s"Recipe ${recipe.name} start to run")
    val start = System.nanoTime

    logInfo(s"Recipe ${recipe.name} has ${recipe.steps.length} steps")
    val steps = recipe.steps

    steps.foreach(step => step.run(ctx))

    logInfo("Recipe %s finished, took %f s".format(recipe.name, (System.nanoTime - start) / 1e9))
  }

}