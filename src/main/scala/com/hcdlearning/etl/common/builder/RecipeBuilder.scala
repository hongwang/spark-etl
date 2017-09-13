package com.hcdlearning.etl.common.builder

import com.hcdlearning.etl.common.{ Logging, ETLException }
import com.hcdlearning.etl.common.definitions.Recipe

object RecipeBuilder extends Logging {
  def from(src: String): Recipe = {
    println(src)
    null
  }
}