package com.hcdlearning.buzz

import java.sql.Date
import java.util.UUID

import com.hcdlearning.buzz.DateFormat._

case class QuizResultActivity(archive_date: String,
                              archive_time: UUID,
                              activity: String,
                              quiz_result_group_id: String,
                              result_id: String,
                              key: String,
                              value: String,
                              insert_date: Date) {
  val __archive_time = getDateFromUUID(archive_time)
}
