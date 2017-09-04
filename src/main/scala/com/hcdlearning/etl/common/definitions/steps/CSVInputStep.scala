package com.hcdlearning.etl.common.definitions.steps

import com.hcdlearning.etl.common.execution.ExecuteContext

class CSVInputStep (
  name: String,
  path: String,
  options: Map[String, String] = Map.empty,
  cache: Boolean = false,
  stage: Boolean = false,
  registerTo: String = ""
) extends BaseStep(name, cache, stage, registerTo) {

  templateFields += ("path" -> path)

  override def execute(ctx: ExecuteContext) {

    val spark = ctx.spark

    import spark.implicits._

    var df = spark.read.options(options).csv(getOrElse("path", path))

    ctx.df = df
  }
}