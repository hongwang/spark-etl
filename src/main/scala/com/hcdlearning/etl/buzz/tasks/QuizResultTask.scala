package com.hcdlearning.etl.buzz.tasks

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.datastax.spark.connector._

import com.hcdlearning.etl.buzz.common.DateFormat._
import com.hcdlearning.etl.buzz.common._
import com.hcdlearning.etl.buzz.entities.QuizResultActivity

object QuizResultTask {

  val name = "QuizResultTask"

  def run(spark: SparkSession, ctx: ETLContext) {

    import spark.implicits._
    spark.udf.register("getTimestampFromUUID", UDF.getTimestampFromUUID)

    // 1. Load delta data
    val archiveDate = format(ctx.targetDate, `yyyyMMdd`)
    val rawDeltaData = spark.sparkContext.cassandraTable[QuizResultActivity]("buzz", "quiz_result_activity")
      .where("archive_date = ?", archiveDate)
      .toDS
      .cache

    rawDeltaData.createOrReplaceTempView("raw_delta_quiz_result_activity")

    // 2. Save raw data
    rawDeltaData
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"hdfs://nameservice-01/user/datahub/staging/buzz/${ctx.workflowId}/${name}_raw_delta_data")

    // 3. Format delta data
    val month = format(ctx.targetDate, `yyyyMM`)
    val formattedDeltaData = spark.sql(
      s"""
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
         |    ${month} as month,
         |    year(insert_date) as year
         |FROM raw_delta_quiz_result_activity
       """.stripMargin).cache

    formattedDeltaData.createOrReplaceTempView("formatted_delta_quiz_result_activity")

    // 4. Save formatted data
    formattedDeltaData
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"hdfs://nameservice-01/user/datahub/staging/buzz/${ctx.workflowId}/${name}_formatted_delta_data")

    // 5. Save to raw activity table
    spark.sql(
      s"""
         |INSERT overwrite TABLE buzz.raw_quiz_result_activity
         |SELECT archive_date,
         |  archive_time,
         |  archive_time_t,
         |  activity,
         |  quiz_result_group_id,
         |  result_id,
         |  key,
         |  value,
         |  insert_date,
         |  __insert_time,
         |  ${month} as month
         |FROM buzz.raw_quiz_result_activity
         |WHERE month = '${month}'
         |  AND archive_date != '${archiveDate}'
         |UNION ALL
         |SELECT archive_date,
         |  archive_time,
         |  archive_time_t,
         |  activity,
         |  quiz_result_group_id,
         |  result_id,
         |  key,
         |  value,
         |  insert_date,
         |  __insert_time,
         |  month
         |FROM formatted_delta_quiz_result_activity
      """.stripMargin)

    // 6. Build the origin table
    buildByWhole(spark)
    //buildByYear(spark, ctx)
    //spark.sql("SELECT * FROM buzz.raw_quiz_result").show(999, false)
  }

  def buildByWhole(spark: SparkSession) = {
    spark.sql(
      s"""
         |INSERT overwrite TABLE buzz.raw_quiz_result
         |SELECT quiz_result_group_id,
         |  result_id,
         |  key,
         |  value,
         |  insert_date,
         |  archive_time_t as update_date,
         |  current_timestamp as __insert_time,
         |  year(insert_date) as year
         |FROM (
         |  SELECT * , ROW_NUMBER() OVER (PARTITION BY quiz_result_group_id, result_id, key ORDER BY archive_time_t desc) as row_no
         |  FROM buzz.raw_quiz_result_activity
         |) t where t.row_no = 1
         |DISTRIBUTE BY year(insert_date), month(insert_date), day(insert_date)
      """.stripMargin)
  }

  def buildByYear(spark: SparkSession, ctx: ETLContext) = {
    spark.sql(
      s"""
         |INSERT overwrite TABLE buzz.raw_quiz_result
         |SELECT quiz_result_group_id,
         |  result_id,
         |  key,
         |  value,
         |  insert_date,
         |  update_date,
         |  __insert_time,
         |  year
         |FROM (
         |  ---- Existing Data
         |  SELECT m.quiz_result_group_id,
         |    m.result_id,
         |    m.key,
         |    (CASE WHEN u.value IS NULL THEN m.value ELSE u.value END) as value,
         |    m.insert_date,
         |    (CASE WHEN u.update_date IS NULL THEN m.update_date ELSE u.update_date END) as update_date,
         |    m.__insert_time,
         |    m.year
         |  FROM buzz.raw_quiz_result m
         |  LEFT JOIN (
         |    SELECT quiz_result_group_id,
         |      result_id,
         |      key,
         |      value,
         |      archive_time_t as update_date,
         |      __insert_time,
         |      year
         |    FROM (
         |      SELECT * , ROW_NUMBER() OVER (PARTITION BY quiz_result_group_id, result_id, key ORDER BY archive_time_t desc) as row_no
         |      FROM formatted_delta_quiz_result_activity
         |      WHERE insert_date < "${ctx.targetDateStr}"
         |    ) t WHERE t.row_no = 1
         |  ) u ON m.quiz_result_group_id = u.quiz_result_group_id
         |      AND m.result_id = u.result_id
         |      AND m.key = u.key
         |      AND m.year = u.year
         |  WHERE m.year IN (
         |    SELECT distinct year
         |    FROM formatted_delta_quiz_result_activity
         |  ) AND m.insert_date < "${ctx.targetDateStr}"
         |  UNION ALL
         |  ---- New Data
         |  SELECT quiz_result_group_id,
         |    result_id,
         |    key,
         |    value,
         |    insert_date,
         |    archive_time_t as update_date,
         |    __insert_time,
         |    year
         |  FROM (
         |    SELECT * , ROW_NUMBER() OVER (PARTITION BY quiz_result_group_id, result_id, key ORDER BY archive_time_t desc) as row_no
         |    FROM formatted_delta_quiz_result_activity
         |    WHERE insert_date >= "${ctx.targetDateStr}"
         |  ) t where t.row_no = 1
         |)
       """.stripMargin)
  }
}
