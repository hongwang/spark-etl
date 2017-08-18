package com.hcdlearning.common.templates

abstract class BaseTemplateEngine() {
  private val TOKEN_PIPE = '|'
  private val LEFT_PARENTHESIS = '('

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

    filters.fold(value) { (v, filterExpr) => {
        if (filterExpr.indexOf(LEFT_PARENTHESIS) < 0) {
          Filters(filterExpr, v)
        } else {
          val Array(filterName, args @ _*) = filterExpr.replace(LEFT_PARENTHESIS, ',')
            .replace(")", "")
            .split(',')
            .map(_.trim)
            .filter(!_.isEmpty)

          //Filters.get(filterName)(v, args: _*)
          Filters(filterName, v, args)
        }
      }
    }
  }
}