package com.hcdlearning.common.steps

import com.hcdlearning.common.{ Logging, ExecuteContext }
import org.apache.spark.sql.{DataFrame, SparkSession}

class CassandraInputStep (
  keyspaceName: String,
  tableName: String,
  whereCql: String,
  cache: Boolean,
  registerTo: String
) extends BaseStep with Logging {

  def this(
    keyspaceName: String, 
    tableName: String) = {
    this(keyspaceName, tableName, "", false, "")
  }

  override def execute(ctx: ExecuteContext) {
    //println("Execute CassandraInputStep")
    logger.info("Execute CassandraInputStep")

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

    if (cache) {
      df = df.cache()
    }

    if (!registerTo.isEmpty) {
      df.createOrReplaceTempView(registerTo)
    }

    ctx.df = df
  }
}