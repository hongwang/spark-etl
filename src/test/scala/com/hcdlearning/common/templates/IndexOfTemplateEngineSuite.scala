package com.hcdlearning.common.templates

import org.scalatest.FunSuite

class IndexOfTemplateEngineSuite extends FunSuite {

  test("basic render") {
    val engine = new IndexOfTemplateEngine()
    val content = "Hello {name}, you are {age} years old."
    val values = Map("name" -> "Hong", "age" -> "18")
    val expected = "Hello Hong, you are 18 years old."

    val actual = engine.render(content, values)

    assert(actual == expected)
  }

}