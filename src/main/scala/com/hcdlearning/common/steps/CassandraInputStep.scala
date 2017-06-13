package com.hcdlearning.common.steps

import com.hcdlearning.common.ExecuteContext
import org.apache.spark.sql.{DataFrame, SparkSession}

class CassandraInputStep (
  name: String,
  keyspaceName: String,
  tableName: String,
  whereCql: String = "",
  coalesce: Option[Int] = None,
  cache: Boolean = false,
  registerTo: String = ""
) extends BaseStep(name, cache, registerTo) {

  override def execute(ctx: ExecuteContext) {

    val spark = ctx.spark

    import spark.implicits._

    var df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keyspaceName, "table" -> tableName, "pushdown" -> "true"))
      .load()

    if (!whereCql.isEmpty) {
      df = df.filter(whereCql)
    }

    if (coalesce.isDefined) {
      df = df.coalesce(coalesce.get)
    }

    ctx.df = df
  }
}