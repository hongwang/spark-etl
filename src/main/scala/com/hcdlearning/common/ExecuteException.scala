package com.hcdlearning.common

class ExecuteException(
  message: String, 
  cause: Throwable
) extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}