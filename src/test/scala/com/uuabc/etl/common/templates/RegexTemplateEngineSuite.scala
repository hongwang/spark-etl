package com.uuabc.etl.common.templates

import com.uuabc.etl.SparkFunSuite

class RegexTemplateEngineSuite extends SparkFunSuite {

  test("basic render") {
    val engine = new RegexTemplateEngine()
    val content = "Hello {name}."
    val values = Map("name" -> "Hong", "age" -> "18")
    val expected = "Hello Hong."

    val actual = engine.render(content, values)

    assert(actual == expected)
  }

  test("missing render") {
    val engine = new RegexTemplateEngine()
    val content = "Hello {name}, you are {age} years old, you like {what}."
    val values = Map("name" -> "Hong", "age" -> "18")
    val expected = "Hello Hong, you are 18 years old, you like {what}."

    val actual = engine.render(content, values)

    assert(actual == expected)
  }

  test("yyyyMMdd filter") {
    val engine = new RegexTemplateEngine()
    val content = "Hello {name}, you were born on {birthday | yyyyMMdd}."
    val values = Map("name" -> "Hong", "birthday" -> "2000-01-02")
    val expected = "Hello Hong, you were born on 20000102."

    val actual = engine.render(content, values)

    assert(actual == expected)
  }
}