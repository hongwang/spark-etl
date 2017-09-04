package com.hcdlearning.etl.common.execution

class ExecuteException(
  message: String, 
  cause: Throwable
) extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}