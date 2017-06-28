package com.hcdlearning.common.definitions.steps

import com.hcdlearning.common.execution.ExecuteContext

class SQLOutputStep(
  name: String,
  sql: String
) extends BaseStep(name) {

  override def execute(ctx: ExecuteContext) {
    ctx.spark.sql(sql)
  }

}