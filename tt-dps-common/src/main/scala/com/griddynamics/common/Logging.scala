package com.griddynamics.common

import org.apache.logging.log4j.{LogManager, Logger}


trait Logging {

  protected val logger: Logger = LogManager.getLogger(this.getClass)

}
