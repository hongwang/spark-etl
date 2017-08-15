package com.hcdlearning.apps

import com.hcdlearning.common.SparkSupported
import com.hcdlearning.common.execution.{ ExecuteContext, ExecuteEngine }
import com.hcdlearning.common.definitions.Recipe
import com.hcdlearning.common.definitions.steps._

object BuzzPromotionBuilding extends App with SparkSupported {

  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

    // Shirley	15601831863	bPHxXqi
    // 王怡	18721364917	bi5RAir
    // 小刘	15161163116	aYGSpAh
    // 小钟	15962776461	cy8UNKA
    // 小麦	15768029200	aKN8TKg
    // 小缪	18362153673	bA6YX9h
    // 小王	15216801867	dgLg8TK
    // Ben	13524661245	bz4QMeH

    val format_sql = s"""
      |SELECT ite.member_id, 
      |  ite.application_id, 
      |  ite.name, 
      |  ite.gender, 
      |  ite.mobile, 
      |  ite.channel, 
      |  ite.regist_date,
      |  g.grade AS education_grade,
      |  s.member_id AS inviter_member_id,
      |  s.name AS inviter_name,
      |  itd.invite_code,
      |  itd.insert_date AS invite_date,
      |  current_timestamp AS __insert_time
      |FROM reg_invitee_member AS ite
      |  INNER JOIN reg_invited_member AS itd
      |    ON ite.member_id = itd.invitee_member_id
      |  INNER JOIN reg_setting AS s
      |    ON itd.invite_code = s.invite_code
      |  LEFT JOIN reg_grade AS g
      |    ON ite.member_id = g.member_id
    """.stripMargin

    val grade_sql = s"""
      |SELECT member_id, grade 
      |FROM (
      |  SELECT member_id, 
      |    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY update_date DESC) as row_no
      |  FROM reg_eductaion
      |) t WHERE t.row_no = 1
    """.stripMargin

    val steps: List[BaseStep] = new CassandraInputStep(
        "load_invited_member", 
        "sso", 
        "invited_member", 
        s"""
          |invite_code in ('bPHxXqi', 'bi5RAir', 'aYGSpAh', 'cy8UNKA', 'aKN8TKg', 'bA6YX9h', 'dgLg8TK', 'bz4QMeH') 
          |and insert_date between '{target_date}' and date_add('{target_date}', 1)""".stripMargin,
        cache=true,
        registerTo="reg_invited_member"
      ) :: new VectorizeStep(
        "extract_member_ids",
        "invitee_member_id",
        "invitee_member_ids",
        VectorizeMode.MKSTRING_WITH_QUOTES
      ) :: new CassandraInputStep(
        "load_invitee_member", 
        "sso", 
        "member", 
        "member_id in ({invitee_member_ids})",
        cache=true,
        registerTo="reg_invitee_member"
      ) :: new CassandraInputStep(
        "load_education", 
        "buzz", 
        "education", 
        "member_id in ({invitee_member_ids})",
        registerTo="reg_eductaion"
      ) ::new SQLTransStep(
        "get_lastest_grade",
        grade_sql,
        cache=true,
        registerTo = "reg_grade"
      ):: new SQLTransStep(
        "format_promotion",
        format_sql,
        registerTo = "reg_formatted_promotion"
      ):: Nil

    val topotaxy = Seq(
      "load_invited_member" -> "extract_member_ids",
      "extract_member_ids" -> "load_invitee_member",
      "load_invitee_member" -> "load_education",
      "load_education" -> "get_lastest_grade",
      "get_lastest_grade" -> "format_promotion"
    )

    val recipe = Recipe("buzz-promotion-building", steps, topotaxy)
    val ctx = ExecuteContext(spark, params)
    ExecuteEngine.run(ctx, recipe)
  }
}