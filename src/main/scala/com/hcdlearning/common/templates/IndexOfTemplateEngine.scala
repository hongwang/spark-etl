package com.hcdlearning.common.templates

class IndexOfTemplateEngine() extends BaseTemplateEngine {
  type SegmentList = List[(Int, String, Int)]

  private def matchData(content: String, from: Int, list: SegmentList): SegmentList = {
    val end = content.lastIndexOf("}", from)
    if (end == -1) 
      return list

    val begin = content.lastIndexOf("{", end)
    if (begin == -1)
      return list

    val template = content.substring(begin, end + 1)
    matchData(content, begin - 1, (begin, template, end + 1) :: list)
  }

  def render(content: String, templateContext: Map[String, String]): String = {
    val sb = new StringBuilder(content.length)
    var prev = 0

    for ((begin, template, end) <- matchData(content, content.length, Nil)) {
      sb.append(content.substring(prev, begin))
      val identifier = template.substring(1, template.length-1)
      val value = eval(identifier, templateContext, template)
      sb.append(value)
      prev = end
    }

    sb.append(content.substring(prev, content.length))
    sb.toString
  }
}