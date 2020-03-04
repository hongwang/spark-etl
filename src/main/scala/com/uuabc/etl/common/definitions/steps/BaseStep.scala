package com.uuabc.etl.common.definitions.steps

import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}
import com.uuabc.etl.common.Logging
import com.uuabc.etl.common.templates.renderEngine
import com.uuabc.etl.common.execution.{ExecuteContext, ExecuteException}

abstract class BaseStep(
  val name: String
) extends Logging {


  protected val templateFields: MutableMap[String, String] = MutableMap()

  final def renderTemplates(ctx: ExecuteContext): Unit = {
    if (templateFields.isEmpty) return

    val templateContext = ctx.getProps + ("step_name" -> name)

    for ((k, v) <- templateFields) {
      val rendered = renderEngine.render(v, templateContext)
      templateFields(k) = rendered
      logInfo(s"render $k: $v => $rendered")
    }
  }

  final def getOrElse(fieldName: String, defaultVal: String): String = {
    templateFields.getOrElse(fieldName, defaultVal)
  }


  // for Topotaxy only
  val upstreamSteps: ArrayBuffer[BaseStep] = ArrayBuffer.empty[BaseStep]

  final def setUpstream(step: BaseStep) {
    upstreamSteps += step
  }

  protected def runCore(ctx: ExecuteContext): Unit

  final def run(ctx: ExecuteContext) {
    logInfo(s"start execute $name")

    try {
      renderTemplates(ctx)

      runCore(ctx)
    } catch {
      case e: Throwable =>
        logError("execute step failed", e)
        throw new ExecuteException(s"Execute failed in $name", e)
    }

    logInfo(s"end execute $name")
  }
}
