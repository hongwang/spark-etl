package com.hcdlearning.common.steps

import com.hcdlearning.common.{ Logging, ExecuteContext }
import org.apache.spark.sql.{SaveMode, SparkSession}

class ParquetOutputStep(
  mode: String,
  path: String
) extends BaseStep with Logging {

  override def execute(ctx: ExecuteContext) {
    logger.info("Execute ParquetOutputStep")

    ctx.df.write.mode(mode).parquet(path)
  }

}