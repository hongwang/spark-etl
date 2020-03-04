package com.uuabc.etl.common.definitions.steps

import com.typesafe.config.Config

object ConfigImplicits {
  implicit class OptionConfig(config: Config) {

    def getStringOpt(key: String): Option[String] = getOpt[String](key, config.getString)
    def getIntOpt(key: String): Option[Int] = getOpt[Int](key, config.getInt)
    def getBooleanOpt(key: String): Option[Boolean] = getOpt[Boolean](key, config.getBoolean)

    def getBooleanOrDefault(key: String, defautVal: Boolean): Boolean = {
      getBooleanOpt(key) match {
        case Some(value) => value
        case _ => defautVal
      }
    }

    private def getOpt[T](key: String, method: String => T) = if (config.hasPath(key)) {
      Some(method(key))
    } else {
      None
    }

  }
}
