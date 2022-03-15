package com.griddynamics.crud

import com.griddynamics.common.ConfigUtils.pipelineConfigs
import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.SnowflakeUtils.JoinCriteria
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.{SaveMode, TableFunction}

object SampleCrud {

  def insertSampleIndustryCode(numRecord: Int): Unit = {

    SnowflakeUtils.writeToTable(
      session =>
        session
          .tableFunction(TableFunction("GENERATE_INDUSTRIES"), lit(numRecord)),
      SaveMode.Overwrite,
      pipelineConfigs.getOrElse(
        "industry-code",
        throw new Error("Table not found")
      )
    )
    SnowflakeUtils.writeFromTableToTable(
      pipelineConfigs
        .getOrElse("industry-code", throw new Error("Table not found")),
      simonTestDataframe =>
        simonTestDataframe.where(
          contains(col("districtCode"), lit("L"))
            .or(contains(col("districtCode"), lit("D")))
        ),
      pipelineConfigs
        .getOrElse("industry-code-l-or-d", throw new Error("Table not found")),
      SaveMode.Overwrite
    )

  }

  def performUpdate(): Unit = {
    SnowflakeUtils.update(
      pipelineConfigs
        .getOrElse("industry-code-l-or-d", throw new Error("Table not found")),
      condition = startswith(lower(col("districtCode")), lit("d")),
      assignments =
        Map("sizeInSquareMeters" -> col("sizeInSquareMeters") * lit(1000))
    )
  }

  def performMerge(): Unit = {
    val sourceDf = sessionManager.get.tableFunction(
      TableFunction("GENERATE_INDUSTRIES"),
      lit(10000)
    )
    val firstTwoLectersDistrictCode: JoinCriteria = (source, destination) => {
      substring(source("districtCode"), lit(0), lit(2))
        .equal_to(substring(destination("districtCode"), lit(0), lit(2)))
    }

    SnowflakeUtils.merge(
      pipelineConfigs
        .getOrElse("industry-code", throw new Error("Table not found")),
      session => sourceDf,
      firstTwoLectersDistrictCode,
      notMatchedOperation = merge =>
        merge.insert(
          sourceDf.schema.fields
            .map(field => (field.name, sourceDf(field.name)))
            .toMap
        )
    )
  }

}
