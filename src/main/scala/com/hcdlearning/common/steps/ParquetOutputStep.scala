package com.hcdlearning.common.steps

import com.hcdlearning.common.ExecuteContext
import org.apache.spark.sql.SparkSession

class ParquetOutputStep(
  name: String,
  mode: String,
  path: String
) extends BaseStep(name) {

  val template_fields = Seq("path")

  override def execute(ctx: ExecuteContext) {
    ctx.df.write.mode(mode).parquet(path)
  }

}