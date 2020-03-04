package com.uuabc.etl.common.definitions.steps

import org.apache.spark.sql.DataFrame
import com.uuabc.etl.common.execution.ExecuteContext

trait IUpstreamSupported {
  val upstream: Option[String]

  def getUpstreamOrLastDF(ctx: ExecuteContext): DataFrame = upstream match {
      case Some(specifiedName) =>
        ctx.pop(specifiedName)
      case _ => ctx.df
    }
}
