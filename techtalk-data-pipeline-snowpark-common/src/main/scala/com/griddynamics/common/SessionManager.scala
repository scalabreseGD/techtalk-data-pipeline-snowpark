package com.griddynamics.common


import com.snowflake.snowpark.Session

import java.util.concurrent.atomic.AtomicReference

class SessionManager(val connProperties:Map[String,String]) extends Logging {

  private var sessionReference: AtomicReference[Session] = createInternal
  private def createInternal: AtomicReference[Session] = {
    val session = Session.builder.configs(connProperties).create
    session.setQueryTag("Snowpark-process")
    sessionReference = new AtomicReference[Session](session)
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info(s"Closing Session ${session.getSessionInfo()}")
      session.close()
    }))
    sessionReference
  }
  def get: Session = {
    try {
      val session = sessionReference.get()
      session.jdbcConnection.isClosed
      logger.info(s"Valid Session Present - providing the existing one ${session.getSessionInfo()}")
      session
    } catch {
      case _:Exception =>
        logger.info("Valid Session Not Present - issuing a new one")
        createInternal.get()
    }
  }
}

object SessionManager{
  def apply(implicit connProperties:Map[String,String]): SessionManager = new SessionManager(connProperties)
}