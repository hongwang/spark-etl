package com.uuabc.etl

import com.uuabc.etl.apps.App
import com.uuabc.etl.common.SparkSupported
import com.uuabc.etl.common.builder.RecipeBuilder
import com.uuabc.etl.common.execution.{ExecuteContext, ExecuteEngine}

object App extends App with SparkSupported {

  private val KEY_RECIPE = "recipe"
  private val KEY_DEPLOY_MODE = "deployMode"

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)

    val recipeConf = params.getOrElse(KEY_RECIPE, throw new IllegalArgumentException(KEY_RECIPE))
    val deployMode = params.getOrElse(KEY_DEPLOY_MODE, "cluster")

    // register UDF

    val recipe = RecipeBuilder.from(recipeConf, deployMode)
    val ctx = ExecuteContext(spark, params)
    ExecuteEngine.run(ctx, recipe)
  }
}
