package com.hcdlearning.etl.common.builder

import com.hcdlearning.SparkFunSuite

class RecipeBuilderSuite extends SparkFunSuite {
  test("simple test") {
    RecipeBuilder.from("hdfs://10.20.32.137:8020/user/spark/recipes/test.json")
  } 
}