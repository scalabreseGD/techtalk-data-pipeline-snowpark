package com.griddynamics.common

import com.snowflake.snowpark._
import com.snowflake.snowpark.functions.lit

@Deprecated
object SnowflakeUtilsOld {

  type DataFrameGenFromSession = Session => DataFrame
  type DataFrameTransformer = DataFrame => DataFrame
  trait JoinCriteria extends ((DataFrame, DataFrame) => Column) {
    def apply(source: DataFrame, target: DataFrame): Column
  }

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

  def writeToTable(
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
    writeToTable(
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

  def delete(
      tableName: String,
      condition: Column,
      joinDataFrame: DataFrameGenFromSession
  )(implicit sessionManager: SessionManager): Unit = {
    generateDfFromTableName(tableName)(sessionManager.get)
      .delete(condition, joinDataFrame(sessionManager.get))
  }

  def merge(
      tableName: String,
      sourceDataFrame: DataFrameGenFromSession,
      joinCriteria: JoinCriteria,
      whenMatchedExtraCondition: Column = lit(true),
      matchedOperation: MatchedClauseBuilder => MergeBuilder = null,
      whenNotMatchedExtraCondition: Column = lit(true),
      notMatchedOperation: NotMatchedClauseBuilder => MergeBuilder = null
  )(implicit sessionManager: SessionManager): Unit = {
    val target = generateDfFromTableName(tableName)(sessionManager.get)
    val source = sourceDataFrame(sessionManager.get)
    val mergeBuilder: MergeBuilder = target.merge(
      source,
      joinCriteria(source, target)
    )

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
