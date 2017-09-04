package com.hcdlearning.etl.apps

trait App {

  protected def parseArgs(args: Array[String]): Map[String, String] = {
    if (args.length < 1) {
      throw new IllegalStateException("some argument must be specified.")
    }

    val prefixWith = """^--(.*)""".r
    args.sliding(2, 1).toList.collect {
      case Array(prefixWith(name), value: String) => (name.toLowerCase, value)
    }.toMap
  }

}