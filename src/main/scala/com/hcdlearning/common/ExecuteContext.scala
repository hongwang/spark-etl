package com.hcdlearning.common

import org.apache.spark.sql.{DataFrame, SparkSession}

private[hcdlearning] class ExecuteContext(
  val spark: SparkSession,
  val debug: Boolean = false
) {

  private[common] var df: DataFrame = _

}