package com.hcdlearning.etl.common.templates

import com.hcdlearning.etl.common.ETLException
import com.hcdlearning.etl.common.util.DateTimeUtils
import com.hcdlearning.etl.common.util.DateTimeUtils.Formats

object Filters {

  private val yyyyMMdd = (value: String) => DateTimeUtils.format(
    DateTimeUtils.parseDate(value), Formats.`yyyyMMdd`)
  private val yyyyMM = (value: String) => DateTimeUtils.format(
    DateTimeUtils.parseDate(value), Formats.`yyyyMM`)

  private val default = (value: String, default_value: String) => if (value == null || value.isEmpty) {
    default_value
  } else {
    value
  }

  private val _fun1Filters = Map(
    "yyyyMMdd" -> yyyyMMdd,
    "yyyyMM" -> yyyyMM
  )

  private val _fun2Filters = Map(
    "default" -> default
  )

  // def get(name: String) = {
  //   _filters.getOrElse(name, throw new ETLException(s"canot find filter: $name"))
  // }

  def apply(name: String, value: String): String = {
    if (_fun1Filters.contains(name)) {
      return _fun1Filters(name)(value)
    }

    throw new ETLException(s"canot find filter: $name")
  }

  def apply(name: String, value: String, args: Seq[String]): String = {
    if (_fun1Filters.contains(name)) {
      return _fun1Filters(name)(value)
    }
    
    if (_fun2Filters.contains(name)) {
      return _fun2Filters(name)(value, args.head)
    }

    throw new ETLException(s"canot find filter: $name")
  }
}