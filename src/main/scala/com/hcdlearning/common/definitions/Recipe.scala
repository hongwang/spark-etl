package com.hcdlearning.common.definitions

import com.hcdlearning.common.Logging
import com.hcdlearning.common.definitions.steps.BaseStep

private[common] case class Recipe(
  name: String,
  steps: Seq[BaseStep]
) {
  
  def topologicalSteps(): Seq[BaseStep] = {
    val sorted = ArrayBuffer.empty[BaseStep]

    if (steps.isEmpty) return sorted

    val unsorted = ArrayBuffer(steps: _*)

    while(unsorted.nonEmpty) {
      acyclic = false

      for(step <- unsorted) {
        for( <- step.upstreamSteps) {

        }
      }

      if(!acyclic) throw new ETLException(s"A cyclic dependency occurred in recipe: ${name}")
    }

    sorted
  }
}

object Recipe extends Logging {

  def apply(name: String, steps: Seq[BaseStep], topotaxy: Map[String, String]): Recipe = {

    buildTopology(steps, topotaxy)
    new Recipe(name, steps)
  }

  private def buildTopology(steps: Seq[BaseStep], topotaxy: Map[String, String]) {
    val keyedSteps = Map(steps.map(step => step.name -> step): _*)

    try {
      topotaxy.foreach((kv: (String, String)) => keyedSteps(kv._2).setUpstream(keyedSteps(kv._1)))
    } catch {
      case e: NoSuchElementException => 
        logError("cannot identify step", e)
        throw e
    }
  }
}