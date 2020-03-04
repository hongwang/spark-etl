package com.uuabc.etl.common.definitions.steps

import com.uuabc.etl.common.execution.ExecuteContext
import org.apache.spark.sql.DataFrame

abstract class BaseTransStep (
  name: String,
  val upstream: Option[String] = None,
  val cache: Boolean = false,
  val persistent: Boolean = false,
  val registerTo: Option[String] = None
) extends BaseStep(name) with IUpstreamSupported with IPostExecution  {

  protected def execute(ctx: ExecuteContext, dataFrame: DataFrame): DataFrame

  override def runCore(ctx: ExecuteContext) {
    val inputDF = getUpstreamOrLastDF(ctx)

    val df = execute(ctx, inputDF)
    require(df != null)

    postExecute(ctx, df)
  }
}
