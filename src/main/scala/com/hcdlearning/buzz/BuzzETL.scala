package com.hcdlearning.buzz

import java.util.UUID

import org.apache.spark.sql.SaveMode
import com.datastax.spark.connector._

import com.hcdlearning.buzz.DateFormat._

object BuzzETL extends SparkSupported {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      throw new IllegalStateException("some argument must be specified.")
    }

    val Array(workflowId, targetDateArg) = args

    val targetDate = parse(targetDateArg, `yyyy-MM-dd`)

    //spark.sql("select * from dw_test.test").show()

    implicit val uuidEncoder = org.apache.spark.sql.Encoders.kryo[UUID]

    // 1. Load daily data
    val archiveDate = format(targetDate, `yyyyMMdd`)
    val deltaActivities = spark.sparkContext.cassandraTable[QuizResultActivity]("buzz", "quiz_result_activity")
      .where("archive_date = ?", archiveDate)
      .toDS
      .cache

    deltaActivities.createOrReplaceTempView("delta_quiz_result_activity")
    deltaActivities.show

    // 2. Save to staging
    deltaActivities
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"hdfs://nameservice-01/user/datahub/staging/buzz/${workflowId}/daily_activities")

    // 3. Save to raw activity table
    val month = format(targetDate, `yyyyMM`)
    spark.sql(
      s"""
         |INSERT overwrite TABLE buzz.raw_quiz_result_activity
         |SELECT archive_date,
         |    archive_time,
         |    __archive_time,
         |    activity,
         |    quiz_result_group_id,
         |    result_id,
         |    key,
         |    value,
         |    insert_date,
         |    ${month} as month
         |FROM buzz.raw_quiz_result_activity
         |WHERE month = '${month}'
         |  AND archive_date != '${archiveDate}'
         |UNION ALL
         |SELECT archive_date,
         |    archive_time,
         |    __archive_time
         |    activity,
         |    quiz_result_group_id,
         |    result_id,
         |    key,
         |    value,
         |    insert_date,
         |    ${month} as month
         |FROM delta_quiz_result_activity
      """.stripMargin)
    //INSERT overwrite TABLE buzz.raw_quiz_result_activity PARTITION (month = '${patitionKey}')

    // 4. Build the origin table
//    spark.sql(
//      s"""
//         |SELECT *,
//         |  rank() OVER (PARTITION BY quiz_result_group_id, result_id, key ORDER BY __archive_time desc) as rank
//         |FROM buzz.raw_quiz_result_activity
//       """.stripMargin).show()


    // 5. Build the theme table


    //println(dailyActivitiesRDD.getNumPartitions)
  }
}
