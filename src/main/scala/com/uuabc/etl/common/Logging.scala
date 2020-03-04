package com.uuabc.etl.common

import org.apache.log4j.{ Logger, Level }

trait Logging {

  @transient private var _logger : Logger = _
  private val _prefix = ">--->"

  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  protected def logger: Logger = {
    if (_logger == null) {
      _logger = Logger.getLogger(logName)
    }
    _logger
  }

  protected def logInfo(msg: => String) {
    if (logger.isInfoEnabled) logger.info(s"${_prefix} ${msg}")
  }

  protected def logInfo(msg: => String, throwable: Throwable) {
    if (logger.isInfoEnabled) logger.info(s"${_prefix} ${msg}", throwable)
  }

  protected def logDebug(msg: => String) {
    if (logger.isDebugEnabled) logger.debug(s"${_prefix} ${msg}")
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (logger.isDebugEnabled) logger.debug(s"${_prefix} ${msg}", throwable)
  }

  protected def logTrace(msg: => String) {
    if (logger.isTraceEnabled) logger.trace(s"${_prefix} ${msg}")
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (logger.isTraceEnabled) logger.trace(s"${_prefix} ${msg}", throwable)
  }

  protected def logWarning(msg: => String) {
    if (logger.isEnabledFor(Level.WARN)) logger.warn(s"${_prefix} ${msg}")
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (logger.isEnabledFor(Level.WARN)) logger.warn(s"${_prefix} ${msg}", throwable)
  }

  protected def logError(msg: => String) {
    if (logger.isEnabledFor(Level.ERROR)) logger.error(s"${_prefix} ${msg}")
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (logger.isEnabledFor(Level.ERROR)) logger.error(s"${_prefix} ${msg}", throwable)
  }
}