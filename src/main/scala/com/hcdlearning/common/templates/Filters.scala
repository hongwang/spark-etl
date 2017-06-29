package com.hcdlearning.common.templates

import com.hcdlearning.common.ETLException
import com.hcdlearning.common.util.DateTimeUtils

object Filters {

  private val yyyyMMdd = (value: String) => DateTimeUtils.format(
    DateTimeUtils.parseDate(value), DateTimeUtils.`yyyyMMdd`)

  private val _filters = Map(
    "yyyyMMdd" -> yyyyMMdd
  )

  def get(name: String) = {
    _filters.getOrElse(name, throw new ETLException(s"canot find filter: $name"))
  }
}