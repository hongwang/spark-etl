package com.hcdlearning.common.definitions

// Recipe -> Phase -> Task -> Step
private[common] case class Recipe(
  val name: String,
  val phases: Seq[Phase]
)

private[common] object Recipe {

  def apply(name: String, steps: Seq[BaseStep]): Recipe = {
    val tasks = Task(steps) :: Nil
    val phases = Phase(tasks) :: Nil
    Recipe(name, phases)
  }
}