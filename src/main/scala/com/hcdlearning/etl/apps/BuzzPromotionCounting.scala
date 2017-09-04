package com.hcdlearning.etl.apps

import com.hcdlearning.etl.common.SparkSupported
import com.hcdlearning.etl.common.execution.{ ExecuteContext, ExecuteEngine }
import com.hcdlearning.etl.common.definitions.Recipe
import com.hcdlearning.etl.common.definitions.steps._

object BuzzPromotionCounting extends App with SparkSupported {

  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

    val load_quiz_activity_sql = s"""
      |SELECT DISTINCT p.inviter_member_id, 
      |  p.channel, 
      |  p.member_id,
      |  p.regist_date,
      |  to_date(r.insert_date) AS active_date
      |FROM buzz.theme_promotion_201708 AS p
      |  INNER JOIN buzz.theme_quiz_result AS r
      |      ON p.member_id = r.member_id
      |WHERE r.month IN (201708, 201709, 201710)
    """.stripMargin

    val calc_active_days_sql = s"""
      |SELECT inviter_member_id, 
      |  channel, 
      |  member_id,
      |  COUNT(CASE WHEN active_date BETWEEN regist_date 
      |    AND DATE_ADD(regist_date, 7) THEN 1 ELSE NULL END) AS w1_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 7) 
      |    AND DATE_ADD(regist_date, 14) THEN 1 ELSE NULL END) AS w2_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 14) 
      |    AND DATE_ADD(regist_date, 21) THEN 1 ELSE NULL END) AS w3_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 21) 
      |    AND DATE_ADD(regist_date, 28) THEN 1 ELSE NULL END) AS w4_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 28) 
      |    AND DATE_ADD(regist_date, 35) THEN 1 ELSE NULL END) AS w5_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 35) 
      |    AND DATE_ADD(regist_date, 42) THEN 1 ELSE NULL END) AS w6_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 42) 
      |    AND DATE_ADD(regist_date, 49) THEN 1 ELSE NULL END) AS w7_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 49) 
      |    AND DATE_ADD(regist_date, 56) THEN 1 ELSE NULL END) AS w8_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 56) 
      |    AND DATE_ADD(regist_date, 63) THEN 1 ELSE NULL END) AS w9_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 63) 
      |    AND DATE_ADD(regist_date, 70) THEN 1 ELSE NULL END) AS w10_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 70) 
      |    AND DATE_ADD(regist_date, 77) THEN 1 ELSE NULL END) AS w11_days,
      |  COUNT(CASE WHEN active_date BETWEEN DATE_ADD(regist_date, 77) 
      |    AND DATE_ADD(regist_date, 84) THEN 1 ELSE NULL END) AS w12_days
      |FROM reg_quiz_activity
      |GROUP BY inviter_member_id, channel, member_id
    """.stripMargin

    val calc_retention_sql = s"""
      |SELECT inviter_member_id, 
      |  channel,
      |  COUNT(CASE WHEN w1_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_1_count,
      |  COUNT(CASE WHEN w2_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_2_count,
      |  COUNT(CASE WHEN w3_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_3_count,
      |  COUNT(CASE WHEN w4_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_4_count,
      |  COUNT(CASE WHEN w5_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_5_count,
      |  COUNT(CASE WHEN w6_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_6_count,
      |  COUNT(CASE WHEN w7_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_7_count,
      |  COUNT(CASE WHEN w8_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_8_count,
      |  COUNT(CASE WHEN w9_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_9_count,
      |  COUNT(CASE WHEN w10_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_10_count,
      |  COUNT(CASE WHEN w11_days >= 1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_11_count,
      |  COUNT(CASE WHEN w12_days >=1 THEN 1 ELSE NULL END) AS hit_1_retention_in_week_12_count,
      |  COUNT(CASE WHEN w1_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_1_count,
      |  COUNT(CASE WHEN w2_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_2_count,
      |  COUNT(CASE WHEN w3_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_3_count,
      |  COUNT(CASE WHEN w4_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_4_count,
      |  COUNT(CASE WHEN w5_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_5_count,
      |  COUNT(CASE WHEN w6_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_6_count,
      |  COUNT(CASE WHEN w7_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_7_count,
      |  COUNT(CASE WHEN w8_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_8_count,
      |  COUNT(CASE WHEN w9_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_9_count,
      |  COUNT(CASE WHEN w10_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_10_count,
      |  COUNT(CASE WHEN w11_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_11_count,
      |  COUNT(CASE WHEN w12_days >=3 THEN 1 ELSE NULL END) AS hit_3_retention_in_week_12_count
      |FROM reg_active_days
      |GROUP BY inviter_member_id, channel
    """.stripMargin

    val load_invitee_stat_sql = s"""
      |SELECT inviter_member_id, 
      |  channel, 
      |  inviter_name,
      |  COUNT(CASE TO_DATE(invite_date) WHEN '{target_date}' THEN 1 ELSE NULL END) AS last_day_new_member_count,
      |  COUNT(1) AS total_new_member_count,
      |  COUNT(CASE education_grade WHEN 1 THEN 1 ELSE NULL END) AS grade_1_count,
      |  COUNT(CASE education_grade WHEN 2 THEN 1 ELSE NULL END) AS grade_2_count,
      |  COUNT(CASE education_grade WHEN 3 THEN 1 ELSE NULL END) AS grade_3_count,
      |  COUNT(CASE education_grade WHEN 4 THEN 1 ELSE NULL END) AS grade_4_count,
      |  COUNT(CASE education_grade WHEN 5 THEN 1 ELSE NULL END) AS grade_5_count,
      |  COUNT(CASE education_grade WHEN 6 THEN 1 ELSE NULL END) AS grade_6_count,
      |  COUNT(CASE education_grade WHEN 7 THEN 1 ELSE NULL END) AS grade_7_count,
      |  COUNT(CASE education_grade WHEN 8 THEN 1 ELSE NULL END) AS grade_8_count,
      |  COUNT(CASE education_grade WHEN 9 THEN 1 ELSE NULL END) AS grade_9_count,
      |  COUNT(CASE gender WHEN 'M' THEN 1 ELSE NULL END) AS male_count,
      |  COUNT(CASE gender WHEN 'F' THEN 1 ELSE NULL END) AS female_count
      |FROM buzz.theme_promotion_201708
      |GROUP BY inviter_member_id, channel, inviter_name
    """.stripMargin

    val combine_sql = s"""
      |SELECT '{target_date}' AS settlement_date,
      |  s.inviter_member_id,
      |  s.channel,
      |  s.inviter_name,
      |  s.last_day_new_member_count,
      |  s.total_new_member_count,
      |  NVL(r.hit_1_retention_in_week_1_count, 0) AS hit_1_retention_in_week_1_count,
      |  NVL(r.hit_1_retention_in_week_2_count, 0) AS hit_1_retention_in_week_2_count,
      |  NVL(r.hit_1_retention_in_week_3_count, 0) AS hit_1_retention_in_week_3_count,
      |  NVL(r.hit_1_retention_in_week_4_count, 0) AS hit_1_retention_in_week_4_count,
      |  NVL(r.hit_1_retention_in_week_5_count, 0) AS hit_1_retention_in_week_5_count,
      |  NVL(r.hit_1_retention_in_week_6_count, 0) AS hit_1_retention_in_week_6_count,
      |  NVL(r.hit_1_retention_in_week_7_count, 0) AS hit_1_retention_in_week_7_count,
      |  NVL(r.hit_1_retention_in_week_8_count, 0) AS hit_1_retention_in_week_8_count,
      |  NVL(r.hit_1_retention_in_week_9_count, 0) AS hit_1_retention_in_week_9_count,
      |  NVL(r.hit_1_retention_in_week_10_count, 0) AS hit_1_retention_in_week_10_count,
      |  NVL(r.hit_1_retention_in_week_11_count, 0) AS hit_1_retention_in_week_11_count,
      |  NVL(r.hit_1_retention_in_week_12_count, 0) AS hit_1_retention_in_week_12_count,
      |  NVL(r.hit_3_retention_in_week_1_count, 0) AS hit_3_retention_in_week_1_count,
      |  NVL(r.hit_3_retention_in_week_2_count, 0) AS hit_3_retention_in_week_2_count,
      |  NVL(r.hit_3_retention_in_week_3_count, 0) AS hit_3_retention_in_week_3_count,
      |  NVL(r.hit_3_retention_in_week_4_count, 0) AS hit_3_retention_in_week_4_count,
      |  NVL(r.hit_3_retention_in_week_5_count, 0) AS hit_3_retention_in_week_5_count,
      |  NVL(r.hit_3_retention_in_week_6_count, 0) AS hit_3_retention_in_week_6_count,
      |  NVL(r.hit_3_retention_in_week_7_count, 0) AS hit_3_retention_in_week_7_count,
      |  NVL(r.hit_3_retention_in_week_8_count, 0) AS hit_3_retention_in_week_8_count,
      |  NVL(r.hit_3_retention_in_week_9_count, 0) AS hit_3_retention_in_week_9_count,
      |  NVL(r.hit_3_retention_in_week_10_count, 0) AS hit_3_retention_in_week_10_count,
      |  NVL(r.hit_3_retention_in_week_11_count, 0) AS hit_3_retention_in_week_11_count,
      |  NVL(r.hit_3_retention_in_week_12_count, 0) AS hit_3_retention_in_week_12_count,
      |  s.grade_1_count,
      |  s.grade_2_count,
      |  s.grade_3_count,
      |  s.grade_4_count,
      |  s.grade_5_count,
      |  s.grade_6_count,
      |  s.grade_7_count,
      |  s.grade_8_count,
      |  s.grade_9_count,
      |  s.male_count,
      |  s.female_count,
      |  current_timestamp AS __insert_time
      |FROM reg_invitee_stat AS s
      |  LEFT JOIN reg_retention AS r
      |    ON s.inviter_member_id = r.inviter_member_id
      |      AND s.channel = r.channel
    """.stripMargin

    val save_sql = s"""
      |SELECT *
      |FROM buzz.stat_promotion_201708
      |WHERE settlement_date != '{target_date}'
      |UNION ALL
      |SELECT *
      |FROM reg_combine
    """.stripMargin

    val steps: List[BaseStep] = new SQLTransStep(
        "load_quiz_activity",
        load_quiz_activity_sql,
        registerTo = "reg_quiz_activity"
      ) :: new SQLTransStep(
        "calc_active_days",
        calc_active_days_sql,
        registerTo = "reg_active_days"
      ) :: new SQLTransStep(
        "calc_retention",
        calc_retention_sql,
        registerTo = "reg_retention"
      ) :: new SQLTransStep(
        "load_invitee_stat",
        load_invitee_stat_sql,
        registerTo = "reg_invitee_stat"
      ) :: new SQLTransStep(
        "combine",
        combine_sql,
        registerTo = "reg_combine"
      ) :: new SQLOutputInPlaceStep(
        "save",
        save_sql,
        "buzz.stat_promotion_201708"
      ) :: Nil

    val topotaxy = Seq(
      "load_quiz_activity"  -> "calc_active_days",
      "calc_active_days"    -> "calc_retention",
      "calc_retention"      -> "load_invitee_stat",
      "load_invitee_stat"   -> "combine",
      "combine"             -> "save"
    )

    val recipe = Recipe("buzz-promotion-counting", steps, topotaxy)
    val ctx = ExecuteContext(spark, params)
    ExecuteEngine.run(ctx, recipe)
  }
}