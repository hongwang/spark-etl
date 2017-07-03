package com.hcdlearning.common.definitions.steps

import com.hcdlearning.common.execution.ExecuteContext
import org.apache.spark.sql.{DataFrame, SparkSession}

class CassandraInputStep (
  name: String,
  keyspaceName: String,
  tableName: String,
  whereCql: String = "",
  coalesce: Option[Int] = None,
  cache: Boolean = false,
  stage: Boolean = false,
  registerTo: String = ""
) extends BaseStep(name, cache, stage, registerTo) {

  templateFields += ("whereCql" -> whereCql)

  override def execute(ctx: ExecuteContext) {

    val spark = ctx.spark

    import spark.implicits._

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

    ctx.df = df
  }
}