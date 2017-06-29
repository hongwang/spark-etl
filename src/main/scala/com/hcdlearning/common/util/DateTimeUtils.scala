package com.hcdlearning.common.util

import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.util.{Date, UUID}

import com.hcdlearning.common.ETLException

object DateTimeUtils {
  final val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L

  private lazy val `yyyy-MM-dd` = new SimpleDateFormat("yyyy-MM-dd")
  private lazy val `yyyy/MM/dd` = new SimpleDateFormat("yyyy/MM/dd")
  
  lazy val `yyyyMMdd` = new SimpleDateFormat("yyyyMMdd")

  private lazy val _formats = Seq(
    `yyyy-MM-dd`,
    `yyyy/MM/dd`
  )

  def timestampFromUUID(uuid: String): Timestamp = {
    timestampFromUUID(UUID.fromString(uuid))
  }

  def timestampFromUUID(uuid: UUID): Timestamp = {
    val timestamp = (uuid.timestamp - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000
    new Timestamp(timestamp)
  }

  def parseDate(value: String): Date = {
    for(format <- _formats) {
      try {
        return format.parse(value)
      } catch {
        case _: ParseException => // No-op
      }
    }

    throw new ETLException(s"cannot find appropriate date format for value: $value")
  }

  def format(value: Date, format: SimpleDateFormat): String = {
    format.format(value)
  }
}