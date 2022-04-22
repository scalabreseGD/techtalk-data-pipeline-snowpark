package com.griddynamics.common

import org.apache.logging.log4j.{Level, LogManager, Logger}
import org.apache.logging.log4j.core.config.Configurator

trait Logging {
  protected val logger: Logger = LogManager.getLogger(this.getClass)
}
