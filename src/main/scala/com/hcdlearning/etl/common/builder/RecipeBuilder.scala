package com.hcdlearning.etl.common.builder

import com.hcdlearning.etl.common.{Logging, ETLException}
import com.hcdlearning.etl.common.definitions.Recipe
import com.hcdlearning.etl.common.util.HDFSClient

object RecipeBuilder extends Logging {
  def from(src: String): Recipe = {
    val content = HDFSClient.readFile(src)
    println(content)

    null
  }
}
