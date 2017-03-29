package com.hcdlearning.buzz

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Date, UUID}

object DateFormat {

  val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L

  val `yyyy-MM-dd` = new SimpleDateFormat("yyyy-MM-dd")
  val `yyyyMMdd` = new SimpleDateFormat("yyyyMMdd")
  val `yyyyMM` = new SimpleDateFormat("yyyyMM")

  def parse(value: String, format: SimpleDateFormat): Date = {
    try {
      format.parse(value)
    } catch {
      case _: ParseException => throw new IllegalArgumentException(s"Invalid value $value")
    }
  }

  def format(value: Date, format: SimpleDateFormat): String = {
    format.format(value)
  }

  private def getTimeFromUUID(uuid: UUID): Long = {
    // https://support.datastax.com/hc/en-us/articles/204226019-Converting-TimeUUID-Strings-to-Dates
    (uuid.timestamp - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000
  }

  def getDateFromUUID(uuid: UUID): Date = {
    new Date(getTimeFromUUID(uuid))
  }
}
