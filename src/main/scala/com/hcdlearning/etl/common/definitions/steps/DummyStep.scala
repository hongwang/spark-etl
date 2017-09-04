package com.hcdlearning.etl.common.definitions.steps

import com.hcdlearning.etl.common.execution.ExecuteContext

class DummyStep (
  name: String
) extends BaseStep(name) {

  override def execute(ctx: ExecuteContext) { }
}