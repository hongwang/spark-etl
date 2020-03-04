package com.uuabc.etl.common

class ETLException(
  message: String, 
  cause: Throwable
) extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}