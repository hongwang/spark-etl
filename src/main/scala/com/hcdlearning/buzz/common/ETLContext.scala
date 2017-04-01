package com.hcdlearning.buzz.common

import java.util.Date
import DateFormat._

case class ETLContext(workflowId: String, targetDate: Date) {
  val targetDateStr = format(targetDate, `yyyy-MM-dd`)
}
