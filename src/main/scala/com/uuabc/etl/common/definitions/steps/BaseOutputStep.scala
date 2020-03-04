package com.uuabc.etl.common.definitions.steps

import com.uuabc.etl.common.execution.ExecuteContext
import org.apache.spark.sql.DataFrame

abstract class BaseOutputStep (
  name: String,
  val upstream: Option[String] = None
) extends BaseStep(name) with IUpstreamSupported {

  protected def execute(ctx: ExecuteContext, df: DataFrame): Unit

  override def runCore(ctx: ExecuteContext) {
    val df = getUpstreamOrLastDF(ctx)

    execute(ctx, df)
  }
}
