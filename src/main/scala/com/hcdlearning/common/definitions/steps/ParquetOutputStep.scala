package com.hcdlearning.common.definitions.steps

import org.apache.spark.sql.SparkSession

import com.hcdlearning.common.execution.ExecuteContext

class ParquetOutputStep(
  name: String,
  mode: String,
  path: String
) extends BaseStep(name) {

  templateFields += ("path" -> path)

  override def execute(ctx: ExecuteContext) {
    val saveTo = getOrElse("path", path)
    logInfo(s"save parquet to $saveTo")

    ctx.df.write.mode(mode).parquet(saveTo)
  }

}