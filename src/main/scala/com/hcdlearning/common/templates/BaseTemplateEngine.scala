package com.hcdlearning.common.templates

abstract class BaseTemplateEngine() {
  private val TOKEN_PIPE = '|'

  def render(content: String, templateContext: Map[String, String]): String

  protected def eval(
    identifier: String, 
    templateContext: Map[String, String], 
    defaultValue: String = ""
  ): String = {

    if (!identifier.contains(TOKEN_PIPE)) {
      return templateContext.getOrElse(identifier.trim, defaultValue)
    }

    val Array(key, filters @ _*) = identifier.split(TOKEN_PIPE).map(_.trim).filter(!_.isEmpty)
    val value = templateContext.getOrElse(key, defaultValue)

    filters.fold(value) { (v, filter) => 
      Filters.get(filter)(v)
    }
  }
}