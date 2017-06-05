package com.hcdlearning.common.templates

abstract class BaseTemplateEngine() {
  def render(content: String, template_context: Map[String, String]): String
}