package com.hcdlearning.etl.buzz.tasks

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.datastax.spark.connector._

import com.hcdlearning.etl.buzz.common.{ETLContext, UDF}
import com.hcdlearning.etl.buzz.entities.Lesson


object LessonTask {
  val name = "LessonTask"

  def run(spark: SparkSession, ctx: ETLContext) {

    import spark.implicits._
    spark.udf.register("getTimestampFromUUID", UDF.getTimestampFromUUID)

    // 1. Load data
    val rawData = spark.sparkContext.cassandraTable[Lesson]("buzz", "lesson")
      .toDS
      .cache

    rawData.createOrReplaceTempView("raw_lesson")

    // 2. Save raw data
    rawData
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"hdfs://nameservice-01/user/datahub/staging/buzz/${ctx.workflowId}/${name}_raw_data")

    // 3. Save to raw lesson table
    spark.sql(
      s"""
         |INSERT overwrite TABLE buzz.raw_lesson
         |SELECT category,
         |  level,
         |  lesson_id,
         |  cast(date as date) as date,
         |  enabled,
         |  new_words_path,
         |  quiz_path,
         |  video_path,
         |  current_timestamp as __insert_time
         |FROM raw_lesson
       """.stripMargin)

    //spark.sql("SELECT * FROM buzz.raw_lesson").show(999, false)
  }
}
