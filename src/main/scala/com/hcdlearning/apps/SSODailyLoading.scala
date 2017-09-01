package com.hcdlearning.apps

import com.hcdlearning.common.SparkSupported
import com.hcdlearning.common.udfs
import com.hcdlearning.common.execution.{ ExecuteContext, ExecuteEngine }
import com.hcdlearning.common.definitions.Recipe
import com.hcdlearning.common.definitions.steps._

object SSODailyLoading extends App with SparkSupported {

  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

    import udfs.implicits._
    //spark.registerAll()
    spark.register("timestamp_from_uuid")

    val format_member_sql = s"""
      |SELECT archive_date,
      |  archive_time,
      |  timestamp_from_uuid(archive_time) as archive_time_t,
      |  member_id,
      |  application_id,
      |  application_set,
      |  name,
      |  user_name,
      |  nick_name,
      |  real_name,
      |  mobile,
      |  mail,
      |  member_type,
      |  is_mail_validated,
      |  id_audit_status,
      |  password,
      |  gender,
      |  birthday,
      |  avatar,
      |  wechat,
      |  country,
      |  province,
      |  city,
      |  channel,
      |  inner_tag,
      |  cast(to_unix_timestamp(regist_date) as timestamp) as regist_date,
      |  cast(to_unix_timestamp(insert_date) as timestamp) as insert_date,
      |  cast(to_unix_timestamp(update_date) as timestamp) as update_date,
      |  '%s' as __data_center,
      |  current_timestamp as __insert_time,
      |  {target_date | yyyyMM} as update_month
      |FROM %s
    """.stripMargin

    val combine_member_sql = s"""
      |SELECT * 
      |FROM reg_formatted_hz_member
      |UNION
      |SELECT * 
      |FROM reg_formatted_sz_member
    """.stripMargin

    val save_member_sql = s"""
      |SELECT * 
      |FROM sso.raw_member_activity
      |WHERE update_month = {target_date | yyyyMM}
      |  AND archive_date != '{target_date | yyyyMMdd}'
      |UNION ALL
      |SELECT * FROM reg_formatted_member
    """.stripMargin

    val steps: List[BaseStep] = new CassandraInputStep(
        "read_hz_member", 
        "sso_archive_hz", 
        "member", 
        "archive_date = '{target_date | yyyyMMdd}'",
        stage=true,
        registerTo="reg_raw_hz_member"
      ) :: new SQLTransStep(
        "format_hz_member",
        format_member_sql.format("hz", "reg_raw_hz_member"),
        registerTo = "reg_formatted_hz_member"
      ) :: new CassandraInputStep(
        "read_sz_member", 
        "sso_archive_sz", 
        "member", 
        "archive_date = '{target_date | yyyyMMdd}'",
        stage=true,
        registerTo="reg_raw_sz_member"
      ) :: new SQLTransStep(
        "format_sz_member",
        format_member_sql.format("sz", "reg_raw_sz_member"),
        registerTo = "reg_formatted_sz_member"
      ) :: new SQLTransStep(
        "combine_member",
        combine_member_sql,
        stage=true,
        registerTo="reg_formatted_member"
      ) :: new SQLTransStep(
        "save_member_act",
        save_member_sql
      ) :: Nil

    val topotaxy = Seq(
      "read_hz_member" -> "format_hz_member",
      "read_sz_member" -> "format_sz_member",
      "format_hz_member" -> "combine_member",
      "format_sz_member" -> "combine_member",
      "combine_member" -> "save_member_act"
    )

    val recipe = Recipe("sso-daily-loading", steps, topotaxy)
    val ctx = ExecuteContext(spark, params)
    ExecuteEngine.run(ctx, recipe)
  }

}
