package com.uuabc.etl.common.templates

class RegexTemplateEngine() extends BaseTemplateEngine {

  private var TAG_REGEX = """\{.+?\}""".r

  def render(content: String, templateContext: Map[String, String]): String = {

    val sb = new StringBuilder(content.length)
    var prev = 0

    for (matchedData <- TAG_REGEX findAllIn content matchData) {
      sb.append(content.substring(prev, matchedData.start))
      val placeholder = matchedData.matched
      val identifier = placeholder.substring(1, placeholder.length-1)
      val value = eval(identifier, templateContext, placeholder)
      sb.append(value)
      prev = matchedData.end
    }

    sb.append(content.substring(prev, content.length))
    sb.toString
  }
}