package com.hcdlearning.common.definitions.steps

import com.hcdlearning.common.execution.ExecuteContext

class DummyStep (
  name: String
) extends BaseStep(name) {

  override def execute(ctx: ExecuteContext) { }
}