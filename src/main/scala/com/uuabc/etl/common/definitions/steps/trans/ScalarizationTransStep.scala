package com.uuabc.etl.common.definitions.steps.trans

import com.uuabc.etl.common.definitions.steps.BaseTransStep
import com.uuabc.etl.common.definitions.steps.trans.ScalarizationMode.ScalarizationMode
import com.uuabc.etl.common.execution.ExecuteContext
import org.apache.spark.sql.DataFrame

class ScalarizationTransStep(
  name: String,
  column: String,
  key: String,
  scalarizationMode: ScalarizationMode
) extends BaseTransStep(name) {

  override def execute(ctx: ExecuteContext, df: DataFrame) = {

    import ctx.spark.implicits._

    val value = scalarizationMode match {
      case ScalarizationMode.MKSTRING => 
        df.select(column).as[String].collect().mkString(",")
      case ScalarizationMode.MKSTRING_WITH_QUOTES => 
        df.select(column).as[String].collect().map(x => s"'$x'").mkString(",")
      case _ => 
        throw new IllegalArgumentException(s"Invalid type $ScalarizationMode")
    }

    ctx.set(key, value)
    df
  }
}

object ScalarizationMode extends Enumeration {

  type ScalarizationMode = Value

  val MKSTRING, MKSTRING_WITH_QUOTES = Value

}