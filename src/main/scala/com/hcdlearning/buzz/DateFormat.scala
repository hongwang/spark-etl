package com.hcdlearning.buzz

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

object DateFormat {

  val `yyyy-MM-dd` = new SimpleDateFormat("yyyy-MM-dd")
  val `yyyyMMdd` = new SimpleDateFormat("yyyyMMdd")

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
}
