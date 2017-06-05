package com.hcdlearning.common.steps

import com.hcdlearning.common.{ Logging, ExecuteContext, ExecuteException }

abstract class BaseStep(
  name: String,
  cache: Boolean = false,
  registerTo: String = ""
) extends Logging {

  protected def execute(ctx: ExecuteContext): Unit

  final def run(ctx: ExecuteContext) {
    logger.info(s"start execute $name")

    try {
      execute(ctx)

      require(ctx.df != null)

      if (ctx.debug) {
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

  def replaceTemplates(s: String, templates: Map[String, String]): String = {
    ""
  }
}