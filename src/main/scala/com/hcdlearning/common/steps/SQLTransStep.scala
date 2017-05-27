package com.hcdlearning.common.steps

import com.hcdlearning.common.ExecuteContext

class SQLTransStep(
  sql: String,
  cache: Boolean = false,
  registerTo: String = ""
) extends BaseStep(cache, registerTo) {

  override def name = "SQLTransStep"

  override def execute(ctx: ExecuteContext) {
    ctx.df = ctx.spark.sql(sql)
  }

}