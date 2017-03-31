package com.hcdlearning.buzz.tasks

import org.apache.spark.sql.SaveMode
import com.datastax.spark.connector._

import com.hcdlearning.buzz.common.DateFormat.{`yyyyMMdd`, format}
import com.hcdlearning.buzz.common._
import com.hcdlearning.buzz.entities.QuizResultGroupActivity

object QuizResultGroupTask extends SparkSupported {

  val name = "QuizResultGroupTask"

  def run(ctx: ETLContext) {

    import spark.implicits._

    // 1. Load daily data
    val archiveDate = format(ctx.targetDate, `yyyyMMdd`)
    val deltaActivities = spark.sparkContext.cassandraTable[QuizResultGroupActivity]("buzz", "quiz_result_group_activity")
      .where("archive_date = ?", archiveDate)
      .toDS
      .cache

    deltaActivities.createOrReplaceTempView("delta_quiz_result_group_activity")
    deltaActivities.show

    // 2. Save to staging
    deltaActivities
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"hdfs://nameservice-01/user/datahub/staging/buzz/${ctx.workflowId}/${name}_daily_activities")


  }
}
