package com.hcdlearning.common.templates

import com.hcdlearning.SparkFunSuite

class RegexTemplateEngineSuite extends SparkFunSuite {

  test("basic render") {
    val engine = new RegexTemplateEngine()
    val content = "Hello {name}."
    val values = Map("name" -> "Hong", "age" -> "18")
    val expected = "Hello Hong."

    val actual = engine.render(content, values)

    assert(actual == expected)
  }

}