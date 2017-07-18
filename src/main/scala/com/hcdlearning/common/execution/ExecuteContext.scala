package com.hcdlearning.common.execution

import org.apache.spark.sql.{DataFrame, SparkSession}

private[hcdlearning] class ExecuteContext private(
  val spark: SparkSession,
  val workflow_id: String,
  val inspect: Boolean = false,
  params: Map[String, String] = Map.empty[String, String]
) {

  private[common] var df: DataFrame = _

  def getParams(): Map[String, String] = {
    params
  }

  def getParam(key: String, defaultVal: String): String = {
    params.getOrElse(key, defaultVal)
  }

  def staging_path: String = params.getOrElse(ExecuteContext.KEY_STAGING_PATH, 
    throw new NoSuchElementException(ExecuteContext.KEY_STAGING_PATH))
}

private[hcdlearning] object ExecuteContext {

  private val KEY_WORKFLOW_ID = "workflow_id"
  private val KEY_STAGING_PATH = "staging_path"
  private val KEY_INSPECT = "inspect"

  def apply(spark: SparkSession, params: Map[String, String]): ExecuteContext = {
    val workflow_id = params.getOrElse(KEY_WORKFLOW_ID, throw new NoSuchElementException(KEY_WORKFLOW_ID))
    val inspect = params.getOrElse(KEY_INSPECT, "false").toBoolean

    if (inspect) {
      spark.sparkContext.setLogLevel("TRACE")

      println("show app arguments")
      params.foreach(pair => println(s"${pair._1} -> ${pair._2}"))
    }

    new ExecuteContext(spark, workflow_id, inspect, params)
  }
}