package com.hcdlearning.common.steps

import com.hcdlearning.common.{ Logging, ExecuteContext }

abstract class BaseStep() {
  def execute(ctx: ExecuteContext): Unit

//   final def run(ctx: ExecuteContext): Boolean = {
//     try {
//       execute(ctx)
//       true
//     } catch {
//       case e: Exception => logger.Error(e)
//       false
//     }
//   }
}