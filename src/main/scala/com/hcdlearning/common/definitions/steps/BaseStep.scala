package com.hcdlearning.common.definitions.steps

import scala.collection.mutable.{ ArrayBuffer, Map => MutableMap }

import com.hcdlearning.common.Logging
import com.hcdlearning.common.definitions.StepState
import com.hcdlearning.common.templates.{ renderEngine, BaseTemplateEngine }
import com.hcdlearning.common.execution.{ ExecuteContext, ExecuteException }

abstract class BaseStep(
  val name: String,
  cache: Boolean = false,
  registerTo: String = ""
) extends Logging {

  private var state = StepState.NONE

  protected val templateFields: MutableMap[String, String] = MutableMap()
  val upstreamSteps: ArrayBuffer[BaseStep] = ArrayBuffer.empty[BaseStep]

  protected def execute(ctx: ExecuteContext): Unit

  final def renderTemplates(ctx: ExecuteContext): Unit = {
    if (templateFields.isEmpty) return

    val templateContext = ctx.getProperties + ("name" -> name)

    for ((k, v) <- templateFields) {
      val rendered = renderEngine.render(v, templateContext)
      templateFields(k) = rendered
      logInfo(s"render $k: $v => $rendered")
    }
  }

  // it should be used if we run steps in parallel
  final def runnable(): Boolean = {  
    upstreamSteps.forall(step => step.state == StepState.SUCCESS)
  }

  final def run(ctx: ExecuteContext) {
    logInfo(s"start execute $name")

    state = StepState.RUNNING
    try {
      renderTemplates(ctx)

      execute(ctx)

      require(ctx.df != null)

      if (ctx.inspect) {
        println(s"show data in $name")
        ctx.df.show(999, false)
      }

      if (cache) {
        ctx.df.cache()
      }

      if (!registerTo.isEmpty) {
        ctx.df.createOrReplaceTempView(registerTo)
      }

      state = StepState.SUCCESS

    } catch {
      case e: Throwable => 
        logError("execute step failed", e)
        state = StepState.FAILED
        throw new ExecuteException(s"Execute failed in $name", e)
    }

    logInfo(s"end execute $name")
  }

  final def setUpstream(step: BaseStep) {
    upstreamSteps += step
  }
}