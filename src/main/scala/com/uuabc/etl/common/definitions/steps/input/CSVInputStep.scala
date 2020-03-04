package com.uuabc.etl.common.definitions.steps.input

import com.uuabc.etl.common.definitions.steps.BaseInputStep
import com.uuabc.etl.common.execution.ExecuteContext

class CSVInputStep (
  name: String,
  path: String,
  options: Map[String, String] = Map.empty,
  cache: Boolean = false,
  stage: Boolean = false,
  registerTo: Option[String] = None
) extends BaseInputStep(name, cache, stage, registerTo) {

  templateFields += ("path" -> path)

  override def execute(ctx: ExecuteContext) = {
    val spark = ctx.spark

    spark.read.options(options).csv(getOrElse("path", path))
  }
}