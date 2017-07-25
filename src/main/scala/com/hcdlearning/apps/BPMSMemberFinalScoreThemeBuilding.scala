package com.hcdlearning.apps

import com.hcdlearning.common.SparkSupported
import com.hcdlearning.common.execution.{ ExecuteContext, ExecuteEngine }
import com.hcdlearning.common.definitions.Recipe
import com.hcdlearning.common.definitions.steps._

object BPMSMemberFinalScoreThemeBuilding extends App with SparkSupported {

  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

    val format_sql = s"""
      |SELECT 
      |  seminar_id,
      |  member_id,
      |  company_id,
      |  company_name,
      |  seminar_description,
      |  semianr_round_count,
      |  seminar_company_count,
      |  seminar_created_time,
      |  (year(seminar_finished_time) * 100 + month(seminar_finished_time)) as seminar_finished_month,
      |  seminar_finished_time,
      |  epic_id,
      |  series_id,
      |  series_title,
      |  series_type,
      |  team_id,
      |  education_id,
      |  education_start_date,
      |  education_end_date,
      |  university,
      |  is_211,
      |  is_985,
      |  qualifications_id,
      |  qualifications,
      |  major_code,
      |  major,
      |  major_type_code,
      |  major_type,
      |  discipline_code,
      |  discipline,
      |  last_period_final_score,
      |  last_period_original_budget,
      |  last_period_original_profit,
      |  last_period_original_som,
      |  last_period_scaled_budget,
      |  last_period_scaled_profit,
      |  last_period_scaled_som,
      |  current_timestamp as __insert_time
      |FROM raw_daily_member_final_score
    """.stripMargin

    val save_sql = s"""
      |INSERT overwrite TABLE bp_marksimos.theme_member_final_score
      |SELECT
      |  seminar_id,
      |  member_id,
      |  company_id,
      |  company_name,
      |  seminar_description,
      |  semianr_round_count,
      |  seminar_company_count,
      |  seminar_created_time,
      |  seminar_finished_month,
      |  seminar_finished_time,
      |  epic_id,
      |  series_id,
      |  series_title,
      |  series_type,
      |  team_id,
      |  education_id,
      |  education_start_date,
      |  education_end_date,
      |  university,
      |  is_211,
      |  is_985,
      |  qualifications_id,
      |  qualifications,
      |  major_code,
      |  major,
      |  major_type_code,
      |  major_type,
      |  discipline_code,
      |  discipline,
      |  last_period_final_score,
      |  last_period_original_budget,
      |  last_period_original_profit,
      |  last_period_original_som,
      |  last_period_scaled_budget,
      |  last_period_scaled_profit,
      |  last_period_scaled_som,
      |  __insert_time
      |FROM bp_marksimos.theme_member_final_score
      |WHERE seminar_finished_month IN (
      |    SELECT DISTINCT seminar_finished_month
      |    FROM formatted_daily_member_final_score
      |  ) AND seminar_id NOT IN (
      |    SELECT DISTINCT seminar_id
      |    FROM formatted_daily_member_final_score
      |  )
      |UNION ALL
      |SELECT
      |  seminar_id,
      |  member_id,
      |  company_id,
      |  company_name,
      |  seminar_description,
      |  semianr_round_count,
      |  seminar_company_count,
      |  seminar_created_time,
      |  seminar_finished_month,
      |  seminar_finished_time,
      |  epic_id,
      |  series_id,
      |  series_title,
      |  series_type,
      |  team_id,
      |  education_id,
      |  education_start_date,
      |  education_end_date,
      |  university,
      |  is_211,
      |  is_985,
      |  qualifications_id,
      |  qualifications,
      |  major_code,
      |  major,
      |  major_type_code,
      |  major_type,
      |  discipline_code,
      |  discipline,
      |  last_period_final_score,
      |  last_period_original_budget,
      |  last_period_original_profit,
      |  last_period_original_som,
      |  last_period_scaled_budget,
      |  last_period_scaled_profit,
      |  last_period_scaled_som,
      |  __insert_time
      |FROM formatted_daily_member_final_score
    """.stripMargin

    val steps: List[BaseStep] = new CSVInputStep(
        "load_csv",
        "hdfs://nameservice-01/user/datahub/staging/marksimos/{workflow_id}/theme_member_final_score.csv.gz",
        options=Map(
          "header" -> "true",
          "compression" -> "gzip",
          "delimiter" -> ";",
          "inferSchema" -> "true"
        ),
        registerTo="raw_daily_member_final_score"
      ) :: new SQLTransStep(
        "format_data",
        format_sql,
        registerTo = "formatted_daily_member_final_score"
      ) :: new SQLTransStep(
        "save",
        save_sql
      ) :: Nil

    val topotaxy = Seq()

    val recipe = Recipe("bpms-member_final_score-theme-building", steps, topotaxy)
    val ctx = ExecuteContext(spark, params)
    ExecuteEngine.run(ctx, recipe)
  }
}