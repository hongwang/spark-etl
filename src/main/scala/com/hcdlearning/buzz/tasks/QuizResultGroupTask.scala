package com.hcdlearning.buzz.tasks

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.datastax.spark.connector._

import com.hcdlearning.buzz.common.DateFormat._
import com.hcdlearning.buzz.common._
import com.hcdlearning.buzz.entities.QuizResultGroupActivity

object QuizResultGroupTask {

  val name = "QuizResultGroupTask"

  def run(spark: SparkSession, ctx: ETLContext) {

    import spark.implicits._
    spark.udf.register("getTimestampFromUUID", UDF.getTimestampFromUUID)

    // 1. Load delta data
    val archiveDate = format(ctx.targetDate, `yyyyMMdd`)
    val rawDeltaData = spark.sparkContext.cassandraTable[QuizResultGroupActivity]("buzz", "quiz_result_group_activity")
      .where("archive_date = ?", archiveDate)
      .toDS
      .cache

    rawDeltaData.createOrReplaceTempView("raw_delta_quiz_result_group_activity")

    // 2. Save raw data
    rawDeltaData
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"hdfs://nameservice-01/user/datahub/staging/buzz/${ctx.workflowId}/${name}_daily_activities")

    // 3. Format delta data
    val month = format(ctx.targetDate, `yyyyMM`)
    val formattedDeltaData = spark.sql(
      s"""
         |SELECT archive_date,
         |    archive_time,
         |    getTimestampFromUUID(archive_time) as archive_time_t,
         |    activity,
         |    member_id,
         |    lesson_id,
         |    type,
         |    quiz_result_group_id,
         |    correct,
         |    wrong,
         |    total,
         |    cast(to_unix_timestamp(start_date) as timestamp) as start_date,
         |    current_timestamp as __insert_time,
         |    ${month} as month,
         |    year(start_date) as year
         |FROM raw_delta_quiz_result_group_activity
       """.stripMargin).cache

    formattedDeltaData.createOrReplaceTempView("formatted_delta_quiz_result_group_activity")

    // 4. Save formatted data
    formattedDeltaData
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"hdfs://nameservice-01/user/datahub/staging/buzz/${ctx.workflowId}/${name}_formatted_delta_data")

    // 5. Save to raw activity group table
    spark.sql(
      s"""
         |INSERT overwrite TABLE buzz.raw_quiz_result_group_activity
         |SELECT archive_date,
         |  archive_time,
         |  archive_time_t,
         |  activity,
         |  member_id,
         |  lesson_id,
         |  type,
         |  quiz_result_group_id,
         |  correct,
         |  wrong,
         |  total,
         |  start_date,
         |  __insert_time,
         |  ${month} as month
         |FROM buzz.raw_quiz_result_group_activity
         |WHERE month = '${month}'
         |  AND archive_date != '${archiveDate}'
         |UNION ALL
         |SELECT archive_date,
         |  archive_time,
         |  archive_time_t,
         |  activity,
         |  member_id,
         |  lesson_id,
         |  type,
         |  quiz_result_group_id,
         |  correct,
         |  wrong,
         |  total,
         |  start_date,
         |  __insert_time,
         |  month
         |FROM formatted_delta_quiz_result_group_activity
      """.stripMargin)

    // 6. Build the origin table
    spark.sql(
      s"""
         |INSERT overwrite TABLE buzz.raw_quiz_result_group
         |SELECT member_id,
         |  lesson_id,
         |  type,
         |  quiz_result_group_id,
         |  correct,
         |  wrong,
         |  total,
         |  start_date,
         |  archive_time_t as update_date,
         |  current_timestamp as __insert_time,
         |  year(start_date) as year
         |FROM (
         |  SELECT * , rank() OVER (PARTITION BY member_id, lesson_id, type, quiz_result_group_id ORDER BY archive_time_t desc) as rank
         |  FROM buzz.raw_quiz_result_group_activity
         |) t where t.rank = 1
         |DISTRIBUTE BY year(start_date), month(start_date), day(start_date)
      """.stripMargin)

    spark.sql(
      s"""
         |SELECT * FROM buzz.raw_quiz_result_group
       """.stripMargin).show(999, false)
  }
}
