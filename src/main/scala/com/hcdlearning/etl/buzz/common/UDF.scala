package com.hcdlearning.etl.buzz.common

import java.util.UUID

object UDF {
  val getTimestampFromUUID = (uuid: String) => DateFormat.getTimestampFromUUID(UUID.fromString(uuid))
}
