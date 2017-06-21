package com.hcdlearning.common.definitions

private[common] object StepState extends Enumeration {

  type StepState = Value

  val NONE, RUNNING, FAILED, SUCCESS = Value

  def isFinished(state: StepState): Boolean = Seq(FAILED, SUCCESS).contains(state)
}