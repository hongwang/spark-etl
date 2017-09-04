package com.hcdlearning.etl.common.definitions.steps

import com.hcdlearning.etl.common.execution.ExecuteContext
import com.hcdlearning.etl.common.definitions.steps.ScalarizationMode.ScalarizationMode

class ScalarizationStep(
  name: String,
  column: String, 
  key: String,
  scalarizationMode: ScalarizationMode
) extends BaseStep(name) {

  override def execute(ctx: ExecuteContext) {

    import ctx.spark.implicits._

    val value = scalarizationMode match {
      case ScalarizationMode.MKSTRING => 
        ctx.df.select(column).as[String].collect().mkString(",")
      case ScalarizationMode.MKSTRING_WITH_QUOTES => 
        ctx.df.select(column).as[String].collect().map(x => s"'$x'").mkString(",")
      case _ => 
        throw new IllegalArgumentException(s"Invalid type $ScalarizationMode")
    }

    ctx.set(key, value)
  }
}

object ScalarizationMode extends Enumeration {

  type ScalarizationMode = Value

  val MKSTRING, MKSTRING_WITH_QUOTES = Value

}