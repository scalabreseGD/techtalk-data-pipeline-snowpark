package com.griddynamics.common

import com.snowflake.snowpark.{DataFrame, Session}

import scala.util.{Failure, Success, Try}

object SnowflakeUtils {

  type TransactionalOperation = Session => Unit
  private val enableTransaction: Session => Session = session => {
    session.jdbcConnection.setAutoCommit(false)
    session
  }
  private val commit: Session => Unit = session =>
    session.jdbcConnection.commit()
  private val rollback: (Throwable, Session) => Unit = (exception, session) => {
    session.jdbcConnection.rollback()
    throw exception
  }
  def createStreamOnTable(
      streamName: String,
      sourceTable: String,
      withReplace: Boolean = false,
      showInitialRows: Boolean = false
  )(implicit sessionManager: SessionManager): Unit = {
    val session = sessionManager.get
    session
      .sql(
        s"CREATE ${if (withReplace) "OR REPLACE" else ""} STREAM $streamName ON TABLE $sourceTable ${if (showInitialRows) "SHOW_INITIAL_ROWS = TRUE" else ""}"
      )
      .count()
  }

  def executeInTransaction(transactionalOperation: TransactionalOperation)(
      implicit sessionManager: SessionManager
  ): Unit = {
    val session = sessionManager.get
    val exec: Session => Unit = (innerSession: Session) =>
      Try {
        transactionalOperation(innerSession)
        innerSession
      } match {
        case Success(value)     => commit(value)
        case Failure(exception) => rollback(exception, innerSession)
      }
    val enriched: Session => Unit = enableTransaction andThen exec
    enriched(session)
  }
}
