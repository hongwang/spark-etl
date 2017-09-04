package com.hcdlearning.etl.common.udfs

import com.hcdlearning.etl.common.util.DateTimeUtils

object DateTimeUDFs {

  val timestamp_from_uuid = (uuid: String) => DateTimeUtils.timestampFromUUID(uuid)

  val fake = (s: Int) => s + 100
}