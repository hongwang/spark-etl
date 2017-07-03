package com.hcdlearning.common.templates

import com.hcdlearning.common.ETLException
import com.hcdlearning.common.util.DateTimeUtils
import com.hcdlearning.common.util.DateTimeUtils.Formats

object Filters {

  private val yyyyMMdd = (value: String) => DateTimeUtils.format(
    DateTimeUtils.parseDate(value), Formats.`yyyyMMdd`)
  private val yyyyMM = (value: String) => DateTimeUtils.format(
    DateTimeUtils.parseDate(value), Formats.`yyyyMM`)

  private val _filters = Map(
    "yyyyMMdd" -> yyyyMMdd,
    "yyyyMM" -> yyyyMM
  )

  def get(name: String) = {
    _filters.getOrElse(name, throw new ETLException(s"canot find filter: $name"))
  }
}