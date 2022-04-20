package com.griddynamics.common

import com.snowflake.snowpark.Session

import scala.util.{Failure, Success, Try}

object SnowflakeUtils {

  type TransactionalOperationWithParams = (Session, Seq[(String,Any)]) => Unit
  type TransactionalOperation = Session => Unit

  sealed trait StreamSourceMode extends scala.AnyRef {
    def objectType: String
  }
  object StreamSourceMode extends scala.AnyRef {
    def apply(mode: String): StreamSourceMode = {
      mode match {
        case "TABLE" => Table
        case "VIEW"  => View
        case "STAGE" => Stage
      }
    }
    object Table extends scala.AnyRef with StreamSourceMode {
      override val objectType = "TABLE"
    }
    object View extends scala.AnyRef with StreamSourceMode {
      override val objectType = "VIEW"
    }

    object Stage extends scala.AnyRef with StreamSourceMode {
      override val objectType = "STAGE"
    }
  }

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
  def createStreamOnObjectType(
      streamName: String,
      sourceObjectName: String,
      withReplace: Boolean = false,
      ifNotExists: Boolean = false,
      showInitialRows: Boolean = false,
      sourceObjectType: StreamSourceMode = StreamSourceMode.Table
  )(implicit session: Session): Unit = {

    val sql = s"CREATE " +
      s" ${if (withReplace) "OR REPLACE" else ""}" +
      s" STREAM " +
      s" ${if (ifNotExists) "IF NOT EXISTS" else ""}" +
      s" $streamName " +
      s" ON ${sourceObjectType.objectType} $sourceObjectName ${if (showInitialRows) "SHOW_INITIAL_ROWS = TRUE"
      else ""}"

    session
      .sql(sql)
      .show()
  }

  def executeInTransaction(transactionalOperation: TransactionalOperation)(
      implicit session: Session
  ): Unit = {
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

  def createStage(
      stageName: String,
      orReplace: Boolean,
      ifNotExists: Boolean,
      directoryEnabled: Boolean = true
  )(implicit session: Session): Unit = {
    val sql = s"CREATE " +
      s" ${if (orReplace) " OR REPLACE " else ""}" +
      s" STAGE " +
      s" ${if (ifNotExists) " IF NOT EXISTS" else ""}" +
      s" $stageName" +
      s" directory = (enable = $directoryEnabled)"
    session
      .sql(sql)
      .show()
  }

  def stageLocalPath(
      stageName: String,
      sourcePath: String,
      destinationPath: String,
      orReplace: Boolean,
      ifNotExists: Boolean
  )(implicit session: Session): Unit = {
    createStage(stageName, orReplace, ifNotExists)
    session.file.put(
      s"file://$sourcePath",
      s"@$stageName${if (destinationPath.startsWith("/")) destinationPath
      else "/" + destinationPath}",
      Map("AUTO_COMPRESS" -> "FALSE")
    )
    session.sql(s"alter stage $stageName refresh").show()
  }

  def waitStreamsRefresh(timeout: Long = 3000): Unit =
    Thread.sleep(timeout) // Wait until the stream got refreshed
}
