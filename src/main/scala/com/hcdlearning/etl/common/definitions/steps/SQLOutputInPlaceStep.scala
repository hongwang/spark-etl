package com.hcdlearning.etl.common.definitions.steps

import org.apache.spark.sql.SaveMode

import com.hcdlearning.etl.common.execution.ExecuteContext

class SQLOutputInPlaceStep(
  name: String,
  sql: String,
  targetTable: String
) extends BaseStep(name) {

  templateFields += ("sql" -> sql)

  override def execute(ctx: ExecuteContext) {
    var df = ctx.spark.sql(getOrElse("sql", sql))

    var tmpTable = s"swap.${name}_${ctx.workflow_id}"
    df.write.mode(SaveMode.Overwrite).saveAsTable(tmpTable)

    var loading_sql = s"""
      |INSERT overwrite TABLE $targetTable
      |SELECT * FROM $tmpTable
    """.stripMargin

    ctx.spark.sql(loading_sql)

    ctx.spark.sql(s"DROP TABLE $tmpTable")
  }

}