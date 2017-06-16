package com.hcdlearning.common

import org.apache.log4j.Logger

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
    if (logger.isInfoEnabled) logger.info(s"$_prefix $msg")
  }

  protected def logDebug(msg: => String) {
    if (logger.isDebugEnabled) logger.debug(s"$_prefix $msg")
  }

  protected def logTrace(msg: => String) {
    if (logger.isTraceEnabled) logger.trace(s"$_prefix $msg")
  }

  protected def logWarning(msg: => String) {
    if (logger.isWarnEnabled) logger.warn(s"$_prefix $msg")
  }

  protected def logError(msg: => String) {
    if (logger.isErrorEnabled) logger.error(s"$_prefix $msg")
  }
}