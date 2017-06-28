package com.hcdlearning.common.templates

import java.text.{ParseException, SimpleDateFormat}
import com.hcdlearning.common.ETLException

object Filters {

  private lazy val `_yyyy-MM-dd` = new SimpleDateFormat("yyyy-MM-dd")
  private lazy val `_yyyyMMdd` = new SimpleDateFormat("yyyyMMdd")

  private val yyyyMMdd = (value: String) => {
    try {
      `_yyyyMMdd`.format(`_yyyy-MM-dd`.parse(value))
    } catch {
      case _: ParseException => throw new IllegalArgumentException(s"Invalid value $value")
    }
  }

  private val _filters = Map(
    "yyyyMMdd" -> yyyyMMdd
  )

  def get(name: String) = {
    _filters.getOrElse(name, throw new ETLException(s"canot find filter: $name"))
  }
}