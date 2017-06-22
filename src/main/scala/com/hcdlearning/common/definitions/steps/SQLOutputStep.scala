package com.hcdlearning.common.definitions.steps

import com.hcdlearning.common.execution.ExecuteContext

class SQLOutputStep(
  name: String,
  sql: String,
  cache: Boolean = false,
  registerTo: String = ""
) extends BaseStep(name, cache, registerTo) {

  override def execute(ctx: ExecuteContext) {
    ctx.spark.sql(sql)
  }

}