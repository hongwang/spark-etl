package com.hcdlearning.etl.common.builder

import com.hcdlearning.SparkFunSuite

class RecipeBuilderSuite extends SparkFunSuite {
  test("simple test") {
    RecipeBuilder.from("123123")
  } 
}