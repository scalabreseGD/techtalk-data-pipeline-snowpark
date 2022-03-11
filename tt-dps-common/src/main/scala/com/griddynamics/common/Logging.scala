package com.griddynamics.common

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger


trait Logging {

  protected val logger: Logger = LogManager.getLogger(this.getClass)

}
