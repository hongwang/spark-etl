package com.hcdlearning.common.steps

import scala.collection.mutable
import com.hcdlearning.common.templates.{ renderEngine, BaseTemplateEngine }
import com.hcdlearning.common.{ Logging, ExecuteContext, ExecuteException }

abstract class BaseStep(
  name: String,
  cache: Boolean = false,
  registerTo: String = ""
) extends Logging {

  protected val templateFields: mutable.Map[String, String] = mutable.Map()

  protected def execute(ctx: ExecuteContext): Unit

  final def renderTemplates(ctx: ExecuteContext): Unit = {
    if (templateFields.isEmpty) return

    val templateContext = ctx.getProperties + ("name" -> name)

    for ((k, v) <- templateFields) {
      val rendered = renderEngine.render(v, templateContext)
      templateFields(k) = rendered
      logger.info(s"render $k: $v => $rendered")
    }
  }

  final def run(ctx: ExecuteContext) {
    logger.info(s"start execute $name")

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
    } catch {
      case e: Exception => 
        logger.error(e)
        throw new ExecuteException(s"Execute failed in $name", e)
    }

    logger.info(s"end execute $name")
  }
}