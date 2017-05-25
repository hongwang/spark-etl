package com.hcdlearning.sso

import com.hcdlearning.buzz.common.{ SparkSupported }
import com.hcdlearning.common.{ ExecuteContext }
import com.hcdlearning.common.steps.{ CassandraLoaderStep }

object SSOStaticETL extends SparkSupported {

  def main(args: Array[String]): Unit = {
    val ctx = new ExecuteContext(spark)

    val step = new CassandraLoaderStep(ctx, "sso", "application")
    val df = step.execute()
    df.show
  }
}
