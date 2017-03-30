package com.hcdlearning.buzz

import java.util.UUID
import org.apache.spark.sql.functions.udf

object UDFs {

  val getTimestampFromUUID = udf((uuid: String) => DateFormat.getTimestampFromUUID(UUID.fromString(uuid)))

}
