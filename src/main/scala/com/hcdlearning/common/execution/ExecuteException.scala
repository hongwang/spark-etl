package com.hcdlearning.common.execution

class ExecuteException(
  message: String, 
  cause: Throwable
) extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}