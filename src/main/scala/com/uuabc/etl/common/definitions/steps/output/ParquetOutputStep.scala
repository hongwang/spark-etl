package com.uuabc.etl.common.definitions.steps.output

import com.uuabc.etl.common.definitions.steps.BaseOutputStep
import org.apache.spark.sql.DataFrame
import com.uuabc.etl.common.execution.ExecuteContext

class ParquetOutputStep(
  name: String,
  mode: String,
  path: String
) extends BaseOutputStep(name) {

  templateFields += ("path" -> path)

  override def execute(ctx: ExecuteContext, df: DataFrame) {
    val saveTo = getOrElse("path", path)
    logInfo(s"save parquet to $saveTo")

    df.write.mode(mode).parquet(saveTo)
  }

}