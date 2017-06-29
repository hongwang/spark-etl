package com.hcdlearning.common.definitions.steps

import com.hcdlearning.common.execution.ExecuteContext

class SQLTransStep(
  name: String,
  sql: String,
  cache: Boolean = false,
  registerTo: String = ""
) extends BaseStep(name, cache, registerTo) {

  templateFields += ("sql" -> sql)

  override def execute(ctx: ExecuteContext) {
    ctx.df = ctx.spark.sql(sql)
  }

}