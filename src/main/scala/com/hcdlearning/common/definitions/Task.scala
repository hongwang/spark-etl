package com.hcdlearning.common.definitions

import com.hcdlearning.common.definitions.steps.BaseStep

private[common] case class Task(
  val steps: Seq[BaseStep]
)

