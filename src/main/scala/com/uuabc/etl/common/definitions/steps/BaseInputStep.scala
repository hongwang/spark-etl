package com.uuabc.etl.common.definitions.steps

import com.uuabc.etl.common.execution.ExecuteContext
import org.apache.spark.sql.DataFrame

abstract class BaseInputStep (
    name: String,
    val cache: Boolean = false,
    val persistent: Boolean = false,
    val registerTo: Option[String] = None
) extends BaseStep(name) with IPostExecution {

  protected def execute(ctx: ExecuteContext): DataFrame

  override def runCore(ctx: ExecuteContext) {
    val df = execute(ctx)
    require(df != null)

    postExecute(ctx, df)
  }
}
