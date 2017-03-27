package com.hcdlearning.buzz

object BuzzETL extends SparkSupported {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      throw new IllegalStateException("some argument must be specified.")
    }

    val Array(targetDate) = args

    spark.sql("select * from dw_test.test").show()
  }
}
