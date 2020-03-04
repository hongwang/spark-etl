package com.uuabc.etl.common.definitions.steps.trans

import com.typesafe.config.Config
import com.uuabc.etl.common.definitions.steps.{BaseStep, BaseTransStep, IStepConfigSupported}
import com.uuabc.etl.common.definitions.steps.ConfigImplicits._
import com.uuabc.etl.common.execution.ExecuteContext
import org.apache.spark.sql.DataFrame

class SQLTransStep(
  name: String,
  sql: String,
  upstream: Option[String] = None,
  cache: Boolean = false,
  stage: Boolean = false,
  registerTo: Option[String] = None
) extends BaseTransStep(name, upstream, cache, stage, registerTo) {

  protected def this() = this(null, null)  // For reflect only

  templateFields += ("sql" -> sql)

  override def execute(ctx: ExecuteContext, df: DataFrame) = {
    ctx.spark.sql(getOrElse("sql", sql))
  }

}

object SQLTransStep extends IStepConfigSupported {
  override def use(config: Config): BaseStep = {
    val name = config.getString("name")
    val sql = config.getString("sql")
    val upstream = config.getStringOpt("upstream")
    val cache = config.getBooleanOrDefault("cache", false)
    val stage = config.getBooleanOrDefault("stage", false)
    val registerTo = config.getStringOpt("registerTo")

    new SQLTransStep(name, sql, upstream, cache, stage, registerTo)
  }
}