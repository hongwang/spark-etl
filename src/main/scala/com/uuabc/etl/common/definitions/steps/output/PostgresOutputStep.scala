package com.uuabc.etl.common.definitions.steps.output

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.uuabc.etl.common.definitions.steps.{BaseOutputStep, BaseStep, IStepConfigSupported}
import com.uuabc.etl.common.definitions.steps.ConfigImplicits._
import com.uuabc.etl.common.execution.ExecuteContext

class PostgresOutputStep(
  name: String,
  table: String,
  upstream: Option[String] = None
) extends BaseOutputStep(name, upstream) {

  protected def this() = this(null, null, null)  // For reflect only

  override def execute(ctx: ExecuteContext, df: DataFrame) {
    val conn_url = ctx.spark.conf.get("spark.pg.url")

    // https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    val prop = new java.util.Properties
    prop.setProperty("truncate", "true")
    prop.setProperty("driver", "org.postgresql.Driver")

    df.write
     .mode(SaveMode.Overwrite) // Append / Overwrite
     .jdbc(conn_url, table, prop)
  }

}

object PostgresOutputStep extends IStepConfigSupported {
  override def use(config: Config): BaseStep = {
    val name = config.getString("name")
    val table = config.getString("table")
    val upstream = config.getStringOpt("upstream")

    new PostgresOutputStep(name, table, upstream)
  }
}
