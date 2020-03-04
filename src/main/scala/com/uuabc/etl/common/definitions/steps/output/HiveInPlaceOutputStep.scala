package com.uuabc.etl.common.definitions.steps.output

import com.uuabc.etl.common.definitions.steps.BaseOutputStep
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.uuabc.etl.common.execution.ExecuteContext

class HiveInPlaceOutputStep(
  name: String,
  sql: String,
  targetTable: String
) extends BaseOutputStep(name) {

  templateFields += ("sql" -> sql)

  override def execute(ctx: ExecuteContext, df: DataFrame) {
    val df = ctx.spark.sql(getOrElse("sql", sql))

    val tmpTable = s"swap.${name}_${ctx.workflow_id}"
    df.write.mode(SaveMode.Overwrite).saveAsTable(tmpTable)

    val loading_sql = s"""
      |INSERT overwrite TABLE $targetTable
      |SELECT * FROM $tmpTable
    """.stripMargin

    ctx.spark.sql(loading_sql)

    ctx.spark.sql(s"DROP TABLE $tmpTable")
  }

}