package com.griddynamics.common

import com.snowflake.snowpark.functions.{col, lit}
import com.snowflake.snowpark.{Column, DataFrame, MatchedClauseBuilder, MergeBuilder, NotMatchedClauseBuilder, SaveMode, Session, Updatable}

import java.util.UUID

object SnowflakeWriter {

  type DataFrameGenFromSession = Session => DataFrame
  type DataFrameTransformer = DataFrame => DataFrame

  private def generateDfFromTableName(
      tableName: String
  ): Session => Updatable = { session =>
    session.table(tableName)
  }

  private def write(
      dataFrame: DataFrame,
      saveMode: SaveMode,
      tableName: String
  ): Unit = {
    dataFrame.write.mode(saveMode).saveAsTable(tableName)
  }

  def writeBronzeLayer(
      dataframeGenerator: DataFrameGenFromSession,
      saveMode: SaveMode,
      tableName: String
  )(implicit sessionManager: SessionManager): Unit = {
    write(dataframeGenerator(sessionManager.get), saveMode, tableName)
  }

  def writeFromTableToTable(
      sourceTable: String,
      transformer: DataFrameTransformer,
      destinationTableName: String,
      saveMode: SaveMode
  )(implicit sessionManager: SessionManager): Unit = {
    writeBronzeLayer(
      generateDfFromTableName(sourceTable).andThen(transformer),
      saveMode,
      destinationTableName
    )
  }
  def writeFromTableToTable2(
      sourceTablesName: (String, String),
      transformer: (DataFrame, DataFrame) => DataFrame,
      destinationTableName: String,
      saveMode: SaveMode
  )(implicit sessionManager: SessionManager): Unit = {
    val session = sessionManager.get
    write(
      transformer(
        session.table(sourceTablesName._1),
        session.table(sourceTablesName._2)
      ),
      saveMode,
      destinationTableName
    )
  }

  def update(
      tableName: String,
      condition: Column,
      assignments: Map[String, Column]
  )(implicit sessionManager: SessionManager): Unit = {
    generateDfFromTableName(tableName)(sessionManager.get)
      .update(assignments = assignments, condition = condition)
  }

  def merge(
      tableName: String,
      sourceDataFrame: DataFrame,
      joinCriteria: Column,
      whenMatchedExtraCondition: Column = lit(true),
      matchedOperation: MatchedClauseBuilder => MergeBuilder = null,
      whenNotMatchedExtraCondition: Column = lit(true),
      notMatchedOperation: NotMatchedClauseBuilder => MergeBuilder = null
  )(implicit sessionManager: SessionManager): Unit = {
    val mergeBuilder: MergeBuilder =
      generateDfFromTableName(tableName)(sessionManager.get)
        .merge(sourceDataFrame, joinCriteria)

    Option(matchedOperation)
      .map(_.apply(mergeBuilder.whenMatched(whenMatchedExtraCondition)))
      .orElse(
        Option(notMatchedOperation).map(
          _.apply(mergeBuilder.whenNotMatched(whenNotMatchedExtraCondition))
        )
      )
      .getOrElse(mergeBuilder)
      .collect()
  }
}
