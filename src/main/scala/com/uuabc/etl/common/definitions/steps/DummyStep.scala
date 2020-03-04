package com.uuabc.etl.common.definitions.steps
import com.uuabc.etl.common.execution.ExecuteContext

class DummyStep(
  name: String
) extends BaseStep(name) {

  override protected def runCore(ctx: ExecuteContext) = {}

}
