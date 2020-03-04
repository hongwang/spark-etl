package com.uuabc.etl.common.definitions.steps

import com.uuabc.etl.common.Logging
import com.uuabc.etl.common.execution.ExecuteContext
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}

trait IPostExecution extends Logging {

  val name: String
  val cache: Boolean
  val persistent: Boolean
  val registerTo: Option[String]

  def postExecute(ctx: ExecuteContext, df: DataFrame): Unit = {
    ctx.df = df

    if (ctx.inspect) {
      println(s"show data in $name, partitions: ${ctx.df.rdd.getNumPartitions}")
      df.show(100, false)
    }

    if (cache) {
      df.cache()
      ctx.push(name, df)
    }

    if (persistent) {
      val path = new Path(ctx.staging_path + "/" + ctx.workflow_id + "/" + name).toString
      logInfo(s"staging step to $path")

      df.write.mode(SaveMode.Overwrite).parquet(path)
    }

    if (registerTo.isDefined) {
      df.createOrReplaceTempView(registerTo.get)
    }
  }
}
