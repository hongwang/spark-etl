package com.hcdlearning.common.definitions

import scala.collection.mutable.ArrayBuffer

import com.hcdlearning.common.{ Logging, ETLException }
import com.hcdlearning.common.definitions.steps.BaseStep

private[hcdlearning] case class Recipe(
  name: String,
  steps: Seq[BaseStep]
)

private[hcdlearning] object Recipe extends Logging {

  def apply(name: String, steps: Seq[BaseStep], topotaxy: Seq[(String, String)]): Recipe = {
    setTopotaxy(steps, topotaxy)
    val sortedSteps = topologicalSort(steps)
    new Recipe(name, sortedSteps)
  }

  private def setTopotaxy(steps: Seq[BaseStep], topotaxy: Seq[(String, String)]) {
    val keyedSteps = Map(steps.map(step => step.name -> step): _*)

    try {
      topotaxy.foreach((kv: (String, String)) => keyedSteps(kv._2).setUpstream(keyedSteps(kv._1)))
    } catch {
      case e: NoSuchElementException => 
        logError("cannot identify step", e)
        throw e
      case e: Throwable =>
        logError("something wrong", e)
        throw e
    }
  }

  private def topologicalSort(steps: Seq[BaseStep]): Seq[BaseStep] = {
    val sorted = ArrayBuffer.empty[BaseStep]

    if (steps.isEmpty) return sorted

    val unsorted = ArrayBuffer(steps: _*)
    while(unsorted.nonEmpty) {
      var acyclic = false

      for(step <- unsorted.toArray) {
        logDebug(s"enter step: ${step.name}, upstream count: ${step.upstreamSteps.length}")
        val unsortedStep = step.upstreamSteps.find(s => unsorted.contains(s))
        logDebug(s"unsortedStep: ${unsortedStep.isEmpty}")
        if (unsortedStep.isEmpty) {
          acyclic = true
          unsorted -= step
          sorted += step
        }
      }

      if(!acyclic) {
        val msg = "A cyclic dependency occurred in recipe"
        logError(msg)
        throw new ETLException(msg)
      }
    }

    sorted
  }
}