package com.hcdlearning.buzz.tasks

import org.apache.spark.sql.SparkSession

import com.hcdlearning.buzz.common.DateFormat._
import com.hcdlearning.buzz.common.ETLContext

object QuizThemeBuildTask {
  val name = "QuizThemeBuildTask"

  def run(spark: SparkSession, ctx: ETLContext) {

    // 1. flatted title, status and score
    spark.sql(
      s"""
         |SELECT quiz_result_group_id, result_id,
         |  collect_list(group_map['title'])[0] as title,
         |  collect_list(group_map['status'])[0] as status,
         |  collect_list(group_map['score'])[0] as score,
         |	min(insert_date) as insert_date,
         |	max(update_date) as update_date
         |FROM (
         |  SELECT quiz_result_group_id, result_id, map(key, value) as group_map, insert_date, update_date
         |  FROM buzz.raw_quiz_result
         |)
         |GROUP BY quiz_result_group_id, result_id
       """.stripMargin)
      .createOrReplaceTempView("pivoted_quize_result")

    // 2. save it with relevant columns
    spark.sql(
      s"""
         |INSERT overwrite TABLE buzz.theme_quiz_result
         |SELECT m.quiz_result_group_id,
         |  g.member_id,
         |  g.lesson_id,
         |  l.category,
         |  l.level,
         |  g.type,
         |  m.result_id,
         |  m.title,
         |  m.status,
         |  m.score,
         |  m.insert_date,
         |  m.update_date,
         |  current_timestamp as __insert_time,
         |  date_format(m.update_date, "yyyyMM") as month
         |FROM pivoted_quize_result AS m
         |INNER JOIN buzz.raw_quiz_result_group AS g
         |    ON m.quiz_result_group_id = g.quiz_result_group_id
         |INNER JOIN buzz.raw_lesson AS l
         |  ON g.lesson_id = l.lesson_id
       """.stripMargin)

    //spark.sql("SELECT * FROM buzz.theme_quiz_result").show(9999, false)
  }
}
