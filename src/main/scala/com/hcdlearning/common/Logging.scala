package com.hcdlearning.common

import org.apache.log4j.Logger

trait Logging {

  @transient private var _logger : Logger = _

  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  protected def logger: Logger = {
    if (_logger == null) {
      _logger = Logger.getLogger(logName)
    }
    _logger
  }
}