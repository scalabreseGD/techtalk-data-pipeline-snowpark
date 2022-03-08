package com.griddynamics.common

import com.snowflake.snowpark.Session

import java.util.concurrent.atomic.AtomicReference

class SessionManager(val connProperties:Map[String,String]) {

  private var sessionReference: AtomicReference[Session] = createInternal
  private def createInternal: AtomicReference[Session] = {
    val session = Session.builder.configs(connProperties).create
    sessionReference = new AtomicReference[Session](session)
    sessionReference
  }
  def get: Session = {
    try {
      sessionReference.get().jdbcConnection.isClosed
      sessionReference.get()
    } catch {
      case _:Exception => createInternal.get()
    }
  }
}

object SessionManager{
  def apply(implicit connProperties:Map[String,String]): SessionManager = new SessionManager(connProperties)
}