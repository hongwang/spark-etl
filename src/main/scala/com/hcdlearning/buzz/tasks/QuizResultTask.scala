package com.hcdlearning.buzz.tasks

import org.apache.spark.sql.SaveMode
import com.datastax.spark.connector._

import com.hcdlearning.buzz.common.DateFormat._
import com.hcdlearning.buzz.common._
import com.hcdlearning.buzz.entities.QuizResultActivity

object QuizResultTask extends SparkSupported {

  val name = "QuizResultTask"

  def run(ctx: ETLContext) {

    import spark.implicits._
    spark.udf.register("getTimestampFromUUID", UDF.getTimestampFromUUID)

    // 1. Load daily data
    val archiveDate = format(ctx.targetDate, `yyyyMMdd`)
    val deltaActivities = spark.sparkContext.cassandraTable[QuizResultActivity]("buzz", "quiz_result_activity")
      .where("archive_date = ?", archiveDate)
      .toDS
      .cache

    deltaActivities.createOrReplaceTempView("delta_quiz_result_activity")

    // 2. Save to staging
    deltaActivities
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"hdfs://nameservice-01/user/datahub/staging/buzz/${ctx.workflowId}/${name}_daily_activities")


    // 3. Save to raw activity table
    val month = format(ctx.targetDate, `yyyyMM`)
    spark.sql(
      s"""
         |INSERT overwrite TABLE buzz.raw_quiz_result_activity
         |SELECT archive_date,
         |    archive_time,
         |    archive_time_t,
         |    activity,
         |    quiz_result_group_id,
         |    result_id,
         |    key,
         |    value,
         |    insert_date,
         |    __insert_time,
         |    ${month} as month
         |FROM buzz.raw_quiz_result_activity
         |WHERE month = '${month}'
         |  AND archive_date != '${archiveDate}'
         |UNION ALL
         |SELECT archive_date,
         |    archive_time,
         |    getTimestampFromUUID(archive_time) as archive_time_t,
         |    activity,
         |    quiz_result_group_id,
         |    result_id,
         |    key,
         |    value,
         |    cast(to_unix_timestamp(insert_date) as timestamp) as insert_date,
         |    current_timestamp as __insert_time,
         |    ${month} as month
         |FROM delta_quiz_result_activity
      """.stripMargin)

    // 4. Build the origin table
    spark.sql(
      s"""
         |INSERT overwrite TABLE buzz.raw_quiz_result
         |SELECT quiz_result_group_id, result_id, key, value, insert_date, archive_time_t as update_date, current_timestamp as __insert_time
         |FROM (
         |  SELECT * , rank() OVER (PARTITION BY quiz_result_group_id, result_id, key ORDER BY archive_time_t desc) as rank
         |  FROM buzz.raw_quiz_result_activity
         |) t where t.rank = 1
       """.stripMargin)
  }
}
