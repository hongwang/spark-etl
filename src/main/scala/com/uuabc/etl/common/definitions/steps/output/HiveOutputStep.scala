package com.uuabc.etl.common.definitions.steps.output

import com.uuabc.etl.common.definitions.steps.BaseOutputStep
import com.uuabc.etl.common.execution.ExecuteContext
import org.apache.spark.sql.DataFrame

class HiveOutputStep(
  name: String,
  sql: String
) extends BaseOutputStep(name) {

  override def execute(ctx: ExecuteContext, df: DataFrame) {
    ctx.spark.sql(sql)
  }

}