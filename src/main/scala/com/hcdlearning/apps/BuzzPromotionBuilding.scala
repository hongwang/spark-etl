package com.hcdlearning.apps

import com.hcdlearning.common.SparkSupported
import com.hcdlearning.common.execution.{ ExecuteContext, ExecuteEngine }
import com.hcdlearning.common.definitions.Recipe
import com.hcdlearning.common.definitions.steps._

object BuzzPromotionBuilding extends App with SparkSupported {

  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

    val format_sql = s"""
      |SELECT ite.member_id, 
      |  ite.application_id, 
      |  ite.name, 
      |  ite.gender, 
      |  ite.mobile, 
      |  ite.channel, 
      |  ite.regist_date,
      |  g.grade AS education_grade,
      |  p.promoter_id AS inviter_member_id,
      |  p.name AS inviter_name,
      |  itd.invite_code,
      |  itd.insert_date AS invite_date,
      |  current_timestamp AS __insert_time
      |FROM reg_invitee_member AS ite
      |  INNER JOIN reg_invited_member AS itd
      |    ON ite.member_id = itd.invitee_member_id
      |  INNER JOIN reg_promoter AS p
      |    ON itd.invite_code = p.invite_code
      |  LEFT JOIN reg_grade AS g
      |    ON ite.member_id = g.member_id
    """.stripMargin

    val grade_sql = s"""
      |SELECT member_id, grade 
      |FROM (
      |  SELECT member_id, grade,
      |    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY update_date DESC) as row_no
      |  FROM reg_eductaion
      |) t WHERE t.row_no = 1
    """.stripMargin

    val save_sql = s"""
      |SELECT *
      |FROM buzz.theme_promotion_201708
      |WHERE invite_date NOT BETWEEN '{target_date}' and date_add('{target_date}', 1)
      |UNION ALL
      |SELECT *
      |FROM reg_formatted_promotion
    """.stripMargin

    val steps: List[BaseStep] = new CSVInputStep(
        "load_promoter_csv",
        "hdfs://nameservice-01/user/datahub/files/buzz/promotion_201708.csv",
        options=Map(
          "header" -> "true",
          "delimiter" -> ";"
        ),
        cache=true,
        registerTo="reg_promoter"
      ) :: new ScalarizationStep(
        "extract_invite_codes",
        "invite_code",
        "invite_codes",
        ScalarizationMode.MKSTRING_WITH_QUOTES
      ) :: new CassandraInputStep(
        "load_invited_member", 
        "sso", 
        "invited_member", 
        s"""
          |invite_code in ({invite_codes}) and 
          |insert_date between '{target_date}' and date_add('{target_date}', 1)""".stripMargin,
        cache=true,
        registerTo="reg_invited_member"
      ) :: new ScalarizationStep(
        "extract_member_ids",
        "invitee_member_id",
        "invitee_member_ids",
        ScalarizationMode.MKSTRING_WITH_QUOTES
      ) :: new CassandraInputStep(
        "load_invitee_member", 
        "sso", 
        "member", 
        "member_id in ({invitee_member_ids})",
        registerTo="reg_invitee_member"
      ) :: new CassandraInputStep(
        "load_education", 
        "buzz", 
        "education", 
        "member_id in ({invitee_member_ids})",
        registerTo="reg_eductaion"
      ) :: new SQLTransStep(
        "get_lastest_grade",
        grade_sql,
        registerTo = "reg_grade"
      ):: new SQLTransStep(
        "format_promotion",
        format_sql,
        registerTo = "reg_formatted_promotion"
      ) :: new SQLOutputInPlaceStep(
        "save",
        save_sql,
        "buzz.theme_promotion_201708"
      ) :: Nil

    val topotaxy = Seq(
      "load_promoter_csv"    -> "extract_invite_codes",
      "extract_invite_codes" -> "load_invited_member",
      "load_invited_member"  -> "extract_member_ids",
      "extract_member_ids"   -> "load_invitee_member",
      "load_invitee_member"  -> "load_education",
      "load_education"       -> "get_lastest_grade",
      "get_lastest_grade"    -> "format_promotion",
      "format_promotion"     -> "save"
    )

    val recipe = Recipe("buzz-promotion-building", steps, topotaxy)
    val ctx = ExecuteContext(spark, params)
    ExecuteEngine.run(ctx, recipe)
  }
}