package com.griddynamics.crud

import com.griddynamics.common.ConfigUtils.pipelineConfigs
import com.griddynamics.common.Implicits.sessionManager
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.{MergeResult, SaveMode, TableFunction}

object SampleCrud {

  def insertSampleIndustryCode(numRecord: Int): Unit = {
    val session = sessionManager.get
    session
      .tableFunction(TableFunction("GENERATE_INDUSTRIES"), lit(numRecord))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(
        pipelineConfigs.getOrElse(
          "industry-code",
          throw new Error("Table not found")
        )
      )

    session
      .table(
        pipelineConfigs
          .getOrElse("industry-code", throw new Error("Table not found"))
      )
      .where(
        contains(col("districtCode"), lit("L"))
          .or(contains(col("districtCode"), lit("D")))
      )
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(
        pipelineConfigs
          .getOrElse("industry-code-l-or-d", throw new Error("Table not found"))
      )
  }

  def performUpdate(): Unit = {
    val session = sessionManager.get
    session
      .table(
        pipelineConfigs
          .getOrElse("industry-code-l-or-d", throw new Error("Table not found"))
      )
      .update(
        assignments =
          Map("sizeInSquareMeters" -> col("sizeInSquareMeters") * lit(1000)),
        condition = startswith(lower(col("districtCode")), lit("d"))
      )
  }

  def performMerge(): Unit = {
    val session = sessionManager.get
    val sourceDf = session.tableFunction(
      TableFunction("GENERATE_INDUSTRIES"),
      lit(10000)
    )

    val target = session
      .table(
        pipelineConfigs
          .getOrElse("industry-code", throw new Error("Table not found"))
      )
    val result: MergeResult = target
      .merge(
        sourceDf,
        substring(sourceDf("districtCode"), lit(0), lit(2))
          === substring(target("districtCode"), lit(0), lit(2))
      )
      .whenNotMatched
      .insert(
        sourceDf.schema.fields
          .map(field => (field.name, sourceDf(field.name)))
          .toMap
      )
      .whenMatched
      .update(Map("sizeInSquareMeters" -> sourceDf("sizeInSquareMeters")))
      .collect()
    print(s"Rows Inserted ${result.rowsInserted} - Rows Updated ${result.rowsUpdated}" )
  }

}
