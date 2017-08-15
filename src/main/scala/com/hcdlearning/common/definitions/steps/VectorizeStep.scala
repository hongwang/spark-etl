package com.hcdlearning.common.definitions.steps

import com.hcdlearning.common.execution.ExecuteContext
import com.hcdlearning.common.definitions.steps.VectorizeMode.VectorizeMode

class VectorizeStep(
  name: String,
  column: String, 
  key: String,
  vectorizeMode: VectorizeMode
) extends BaseStep(name) {

  override def execute(ctx: ExecuteContext) {

    import ctx.spark.implicits._

    val value = vectorizeMode match {
      case VectorizeMode.MKSTRING => 
        ctx.df.select(column).as[String].collect().mkString(",")
      case VectorizeMode.MKSTRING_WITH_QUOTES => 
        ctx.df.select(column).as[String].collect().map(x => s"'$x'").mkString(",")
      case _ => 
        throw new IllegalArgumentException(s"Invalid type $vectorizeMode")
    }

    ctx.set(key, value)
  }
}