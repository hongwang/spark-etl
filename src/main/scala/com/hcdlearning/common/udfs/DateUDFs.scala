package com.hcdlearning.common.udfs

import java.sql.Timestamp
import java.util.UUID

object DateUDFs {
  val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L

  val to_timestamp_from_uuid = (uuid: String) => {
    val timestamp = (UUID.fromString(uuid).timestamp - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000
    new Timestamp(timestamp)
  }

  val fake = (s: Int) => s + 100
}