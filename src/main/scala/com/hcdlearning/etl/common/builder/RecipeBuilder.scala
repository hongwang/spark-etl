package com.hcdlearning.etl.common.builder

import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.hcdlearning.etl.common.{Logging, ETLException}
import com.hcdlearning.etl.common.definitions.Recipe
import com.hcdlearning.etl.common.util.HDFSClient

object RecipeBuilder extends Logging {
  def from(src: String): Recipe = {
    val content = HDFSClient.readFile(src)
    val json = parse(content)
    
    val name = for {
      JObject(child) <- json
      JField("name", JString(name)) <- child
    }

    println(name)
    null
  }
}
