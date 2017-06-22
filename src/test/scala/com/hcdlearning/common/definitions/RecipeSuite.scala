package com.hcdlearning.common.definitions

import com.hcdlearning.SparkFunSuite
import com.hcdlearning.common.ETLException
import com.hcdlearning.common.definitions.steps._

class RecipeSuite extends SparkFunSuite {

  test("simple topological sort") {
    val step1 = new DummyStep("step_1")
    val step2 = new DummyStep("step_2")
    val step3 = new DummyStep("step_3")

    val steps: List[BaseStep] = step1 :: step2 :: step3 :: Nil

    val topotaxy = Seq(
      "step_2" -> "step_1",
      "step_1" -> "step_3"
    )

    val recipe = Recipe("simple_recipe", steps, topotaxy)
    val sortedSteps = recipe.steps

    assert(sortedSteps.indexOf(step2) == 0)
    assert(sortedSteps.indexOf(step1) == 1)
    assert(sortedSteps.indexOf(step3) == 2)
  }

  test("cyclic topological sort") {
    val step1 = new DummyStep("step_1")
    val step2 = new DummyStep("step_2")
    val step3 = new DummyStep("step_3")

    val steps: List[BaseStep] = step1 :: step2 :: step3 :: Nil

    val topotaxy = Seq(
      "step_2" -> "step_1", 
      "step_1" -> "step_3",
      "step_3" -> "step_2"
    )

    assertThrows[ETLException](Recipe("test_recipe", steps, topotaxy))
  }

  test("complex topological sort") {
    val step1 = new DummyStep("step_1")
    val step2 = new DummyStep("step_2")
    val step3 = new DummyStep("step_3")
    val step4 = new DummyStep("step_4")
    val step5 = new DummyStep("step_5")
    val step6 = new DummyStep("step_6")
    val step7 = new DummyStep("step_7")
    val step8 = new DummyStep("step_8")

    val steps: List[BaseStep] = step1 :: step2 :: step3 :: 
      step4 :: step5 :: step6 :: step7 :: step8 :: Nil

    val topotaxy = Seq(
      "step_3" -> "step_6",
      "step_5" -> "step_6",
      "step_6" -> "step_1",
      "step_6" -> "step_2",
      "step_6" -> "step_4",
      "step_1" -> "step_8",
      "step_2" -> "step_8"
    )

    val recipe = Recipe("complex_recipe", steps, topotaxy)
    val sortedSteps = recipe.steps

    assert(sortedSteps.indexOf(step3) < sortedSteps.indexOf(step6))
    assert(sortedSteps.indexOf(step5) < sortedSteps.indexOf(step6))

    assert(sortedSteps.indexOf(step6) < sortedSteps.indexOf(step1))
    assert(sortedSteps.indexOf(step6) < sortedSteps.indexOf(step2))
    assert(sortedSteps.indexOf(step6) < sortedSteps.indexOf(step4))

    assert(sortedSteps.indexOf(step1) < sortedSteps.indexOf(step8))
    assert(sortedSteps.indexOf(step2) < sortedSteps.indexOf(step8))
  }

}