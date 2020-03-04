package com.uuabc.etl.common.util

import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.util.{Date, UUID}
import java.time.LocalDate
import java.time.format.{ DateTimeParseException, DateTimeFormatter }

import com.uuabc.etl.common.ETLException

object DateTimeUtils {
  final val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L

  private lazy val _formats = Seq(
    Formats.`yyyy-MM-dd`,
    Formats.`yyyy/MM/dd`
  )

  def timestampFromUUID(uuid: String): Timestamp = {
    timestampFromUUID(UUID.fromString(uuid))
  }

  def timestampFromUUID(uuid: UUID): Timestamp = {
    val timestamp = (uuid.timestamp - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000
    new Timestamp(timestamp)
  }

  def parseDate(value: String): LocalDate = {
    for(format <- _formats) {
      try {
        return LocalDate.parse(value, format);
      } catch {
        case _: DateTimeParseException => // No-op
      }
    }

    throw new ETLException(s"cannot find appropriate date format for value: $value")
  }

  def format(value: LocalDate, format: DateTimeFormatter): String = {
    format.format(value)
  }


  object Formats {
    lazy val `yyyyMMdd` = DateTimeFormatter.ofPattern("yyyyMMdd");
    lazy val `yyyyMM` = DateTimeFormatter.ofPattern("yyyyMM")
    lazy val `yyyy-MM-dd` = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    lazy val `yyyy/MM/dd` = DateTimeFormatter.ofPattern("yyyy/MM/dd")
  }
}