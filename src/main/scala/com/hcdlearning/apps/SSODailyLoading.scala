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
    spark.registerAll()

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
      |  cast(to_unix_timestamp(regist_date) as timestamp) as insert_date,
      |  cast(to_unix_timestamp(insert_date) as timestamp) as insert_date,
      |  cast(to_unix_timestamp(update_date) as timestamp) as insert_date,
      |  {target_date | yyyyMM} as __update_month,
      |  '%s' as __data_center,
      |  current_timestamp as __insert_time
      |FROM %s
    """.stripMargin

    val format_sz_member_sql = s"""
    """.stripMargin

    val steps: List[BaseStep] = new CassandraInputStep(
        "read_hz_member", 
        "sso_archive_hz", 
        "member", 
        "archive_date = '{target_date | yyyyMMdd}'",
        registerTo = "reg_raw_hz_member"
      ) :: new ParquetOutputStep(
        "stage_hz_member", 
        "Overwrite", 
        "hdfs://nameservice-01/user/datahub/staging/sso/{workflow_id}/{step_name}_raw_data"
      ) :: new SQLTransStep(
        "format_hz_member",
        format_member_sql.format("hz", "reg_raw_hz_member"),
        registerTo = "reg_formatted_hz_member"
      ) :: new CassandraInputStep(
        "read_sz_member", 
        "sso_archive_sz", 
        "member", 
        "archive_date = '{target_date | yyyyMMdd}'",
        registerTo = "reg_raw_sz_member"
      ) :: new ParquetOutputStep(
        "stage_sz_member", 
        "Overwrite", 
        "hdfs://nameservice-01/user/datahub/staging/sso/{workflow_id}/{step_name}_raw_data"
      ) :: new SQLTransStep(
        "format_sz_member",
        format_member_sql.format("sz", "reg_raw_sz_member"),
        registerTo = "reg_formatted_sz_member"
      )  :: Nil

    val topotaxy = Seq(
      "read_hz_member" -> "stage_hz_member",
      "stage_hz_member" -> "format_hz_member",
      "read_sz_member" -> "stage_sz_member",
      "stage_sz_member" -> "format_sz_member"
    )

    val recipe = Recipe("sso-daily-loading", steps, topotaxy)
    val ctx = ExecuteContext(spark, params)
    ExecuteEngine.run(ctx, recipe)
  }

}
