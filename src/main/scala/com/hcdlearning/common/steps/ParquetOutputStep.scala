package com.hcdlearning.common.steps

import com.hcdlearning.common.ExecuteContext
import org.apache.spark.sql.SparkSession

class ParquetOutputStep(
  mode: String,
  path: String
) extends BaseStep {

  override def name = "ParquetOutputStep"

  override def execute(ctx: ExecuteContext) {
    ctx.df.write.mode(mode).parquet(path)
  }

}