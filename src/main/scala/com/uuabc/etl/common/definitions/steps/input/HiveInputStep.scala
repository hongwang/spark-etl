package com.uuabc.etl.common.definitions.steps.input

import com.typesafe.config.Config
import com.uuabc.etl.common.definitions.steps.{BaseInputStep, BaseStep, IStepConfigSupported}
import com.uuabc.etl.common.execution.ExecuteContext

class HiveInputStep(
  name: String,
  sql: String,
  registerTo: Option[String] = None
) extends BaseInputStep(name, registerTo = registerTo) {

  protected def this() = this(null, null, null)  // For reflect only

  templateFields += ("sql" -> sql)

  override def execute(ctx: ExecuteContext) = {
    ctx.spark.sql(getOrElse("sql", sql))
  }
}

object HiveInputStep extends IStepConfigSupported {
  override def use(config: Config): BaseStep = {
    val name = config.getString("name")
    val sql = config.getString("sql")
    val registerTo = Option(config.getString("registerTo"))

    new HiveInputStep(name, sql, registerTo)
  }
}
