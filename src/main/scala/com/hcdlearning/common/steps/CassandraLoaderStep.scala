package com.hcdlearning.common.steps

import org.apache.log4j.Logger
import org.apache.spark.sql._
import com.hcdlearning.common.{ ExecuteContext }

class CassandraLoaderStep (
  ctx: ExecuteContext,
  keyspaceName: String,
  tableName: String,
  whereCql: String
) extends BaseStep {

  def this(
    ctx: ExecuteContext,
    keyspaceName: String, 
    tableName: String) = {
    this(ctx, keyspaceName, tableName, "")
  }

  val spark = ctx.getSpark
  val logger = Logger.getLogger(classOf[CassandraLoaderStep].getName)

  override def execute(): DataFrame = {
    println("Execute CassandraLoaderStep from println")
    logger.info("Execute CassandraLoaderStep from logger")

    import spark.implicits._

    var df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keyspaceName, "table" -> tableName, "pushdown" -> "true"))
      .load()

    if (!whereCql.isEmpty) {
      df = df.filter(whereCql)
    }

    df
  }
}