package com.uuabc.etl.common.definitions.steps.output

import com.typesafe.config.Config
import com.uuabc.etl.common.definitions.steps.{BaseOutputStep, BaseStep, IStepConfigSupported}
import com.uuabc.etl.common.execution.ExecuteContext
import org.apache.spark.sql.DataFrame

class StdoutOutputStep(
  name: String
) extends BaseOutputStep(name) {

  protected def this() = this(null)  // For reflect only

  override protected def execute(ctx: ExecuteContext, df: DataFrame) {
    df.show()
  }
}

object StdoutOutputStep extends IStepConfigSupported {
  override def use(config: Config): BaseStep = {
    val name = config.getString("name")
    new StdoutOutputStep(name)
  }
}