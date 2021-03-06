package com.uuabc.etl.common.execution

import scala.collection.mutable
import org.apache.spark.sql.{DataFrame, SparkSession}

private[uuabc] class ExecuteContext private(
  val spark: SparkSession,
  val workflow_id: String,
  val inspect: Boolean = false,
  params: Map[String, String] = Map.empty[String, String]
) {

  private val localProperties = mutable.Map[String, String](params.toSeq: _*)
  private val stash = mutable.Map.empty[String, DataFrame]

  private[common] var df: DataFrame = _

  def push(stepName: String, df: DataFrame): Unit = {
    stash(stepName) = df
  }

  def pop(stepName: String): DataFrame = {
    stash.getOrElse(stepName,
      throw new NoSuchElementException(s"missing cached DataFrame for step: $stepName"))
  }

  def getProps(): Map[String, String] = {
    localProperties.toMap
  }

  def staging_path: String = localProperties.getOrElse(ExecuteContext.KEY_STAGING_PATH,
    throw new NoSuchElementException(ExecuteContext.KEY_STAGING_PATH))

  def set(key: String, value: String) {
    localProperties(key) = value
  }

}

private[uuabc] object ExecuteContext {

  private val KEY_WORKFLOW_ID = "workflow_id"
  private val KEY_STAGING_PATH = "staging_path"
  private val KEY_INSPECT = "inspect"

  def apply(spark: SparkSession, params: Map[String, String]): ExecuteContext = {
    val workflow_id = params.getOrElse(KEY_WORKFLOW_ID, throw new NoSuchElementException(KEY_WORKFLOW_ID))
    val inspect = params.getOrElse(KEY_INSPECT, "false").toBoolean

    if (inspect) {
      //spark.sparkContext.setLogLevel("TRACE")

      println("show app arguments")
      params.foreach(pair => println(s"${pair._1} -> ${pair._2}"))
    }

    new ExecuteContext(spark, workflow_id, inspect, params)
  }
}