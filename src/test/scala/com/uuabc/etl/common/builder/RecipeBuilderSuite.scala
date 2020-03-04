package com.uuabc.etl.common.builder

import com.uuabc.etl.SparkFunSuite

class RecipeBuilderSuite extends SparkFunSuite {
  test("simple test") {
    val recipeFile = getClass.getResource("/recipes/test.json").getPath
    val recipeBuilder = new RecipeBuilder(recipeFile, "client")
    val recipe = recipeBuilder.build

    assert(recipe.name == "test")
  }

  test("hdfs conf test") {
    val recipeFile = "hdfs://hdfscluster/apps/spark/recipes/test.json"
    val recipeBuilder = new RecipeBuilder(recipeFile)
    val recipe = recipeBuilder.build

    assert(recipe.name == "test")
  }

  test("conf env substitution test") {
    val recipeFile = getClass.getResource("/recipes/test_env.conf").getPath
    val recipeBuilder = new RecipeBuilder(recipeFile, "client")
    val recipe = recipeBuilder.build

    assert(recipe.name == "test_env")
    assert(recipe.steps.size == 3)
    assert(recipe.steps.last.name == "save_to_es")
  }

  test("json env substitution test") {
    val recipeFile = getClass.getResource("/recipes/test_env.json").getPath
    val recipeBuilder = new RecipeBuilder(recipeFile, "client")
    val recipe = recipeBuilder.build

    assert(recipe.name == "test_env")
  }
}