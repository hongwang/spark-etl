package com.hcdlearning.common.definitions.steps

import com.hcdlearning.common.execution.ExecuteContext

class CSVInputStep (
  name: String,
  path: String,
  header: Boolean = false,
  compression: String = "none",
  cache: Boolean = false,
  stage: Boolean = false,
  registerTo: String = ""
) extends BaseStep(name, cache, stage, registerTo) {

  templateFields += ("path" -> path)

  override def execute(ctx: ExecuteContext) {

    val spark = ctx.spark

    import spark.implicits._

    var df = spark.read.option("header", header).csv(getOrElse("path", path))

    ctx.df = df
  }
}