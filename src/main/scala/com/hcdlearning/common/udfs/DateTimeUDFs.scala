package com.hcdlearning.common.udfs

import com.hcdlearning.common.util.DateTimeUtils

object DateTimeUDFs {

  val timestamp_from_uuid = (uuid: String) => DateTimeUtils.timestampFromUUID(uuid)

  val fake = (s: Int) => s + 100
}