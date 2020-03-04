package com.uuabc.etl.common.definitions.steps.input

import com.typesafe.config.Config
import com.uuabc.etl.common.definitions.steps.{BaseInputStep, BaseStep, IStepConfigSupported}
import com.uuabc.etl.common.definitions.steps.ConfigImplicits._
import com.uuabc.etl.common.execution.ExecuteContext

class CassandraInputStep(
  name: String,
  keyspaceName: String,
  tableName: String,
  whereCql: String = "",
  coalesce: Option[Int] = None,
  cache: Boolean = false,
  stage: Boolean = false,
  registerTo: Option[String] = None
) extends BaseInputStep(name, cache, stage, registerTo) {

  protected def this() = this(null, null, null)  // For reflect only

  templateFields += ("whereCql" -> whereCql)

  override def execute(ctx: ExecuteContext) = {

    val spark = ctx.spark

    var df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keyspaceName, "table" -> tableName, "pushdown" -> "true"))
      .load()

    if (!whereCql.isEmpty) {
      df = df.filter(getOrElse("whereCql", whereCql))
    }

    if (coalesce.isDefined) {
      df = df.coalesce(coalesce.get)
    }

    df
  }
}

object CassandraInputStep extends IStepConfigSupported {
  override def use(config: Config): BaseStep = {
    val name = config.getString("name")
    val keyspaceName = config.getString("keyspaceName")
    val tableName = config.getString("tableName")
    val whereCql = config.getString("whereCql")
    val coalesce = config.getIntOpt("coalesce")
    val cache = config.getBooleanOrDefault("cache", false)
    val stage = config.getBooleanOrDefault("stage", false)
    val registerTo = config.getStringOpt("registerTo")

    new CassandraInputStep(
      name,
      keyspaceName,
      tableName,
      whereCql,
      coalesce,
      cache,
      stage,
      registerTo
    )
  }
}