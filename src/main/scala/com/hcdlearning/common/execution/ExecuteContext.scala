package com.hcdlearning.common.execution

import org.apache.spark.sql.{DataFrame, SparkSession}

private[hcdlearning] class ExecuteContext(
  val spark: SparkSession,
  val workflowId: String,
  val inspect: Boolean = false
) {

  private[common] var df: DataFrame = _

  def getProperties(): Map[String, String] = {
    Map(
      "inspect" -> inspect.toString,
      "workflowId" -> workflowId
    )
  }
}