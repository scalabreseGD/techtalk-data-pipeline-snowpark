package com.griddynamics.common

import com.snowflake.snowpark.{DataFrame, Session}

object SnowflakeUtils {

  type TransactionalOperation = Session => Unit
  private val enableTransaction: Session => Session = session => {
    session.jdbcConnection.setAutoCommit(false)
    session
  }
  private val commit: Session => Unit = session =>
    session.jdbcConnection.commit()
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
    enableTransaction
      .andThen(innerSession => {
        transactionalOperation(innerSession)
        innerSession
      })
      .andThen(commit)
      .apply(session)
  }
}
