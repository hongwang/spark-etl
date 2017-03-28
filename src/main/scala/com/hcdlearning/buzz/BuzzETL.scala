package com.hcdlearning.buzz

import com.datastax.spark.connector._

import com.hcdlearning.buzz.DateFormat._

object BuzzETL extends SparkSupported {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      throw new IllegalStateException("some argument must be specified.")
    }

    val Array(workflowId, targetDateArg) = args

    val targetDate = parse(targetDateArg, `yyyy-MM-dd`)

    //spark.sql("select * from dw_test.test").show()

    val dailyActivitiesRDD = spark.sparkContext.cassandraTable("buzz", "quiz_result_activity")
      .where("archive_date = ?", format(targetDate, `yyyyMMdd`))

    println(dailyActivitiesRDD.getNumPartitions)
  }
}
