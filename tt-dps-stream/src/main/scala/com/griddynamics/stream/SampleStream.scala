package com.griddynamics.stream

import com.griddynamics.common.ConfigUtils.pipelineConfigs
import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.SnowflakeUtils
import com.snowflake.snowpark.functions.{col, lit, substring, upper}
import com.snowflake.snowpark.{SaveMode, TableFunction}

object SampleStream {

  def createIndustryCodeStream(): Unit = {
    SnowflakeUtils.createStreamOnTable(
      pipelineConfigs
        .getOrElse("industry-code-stream", throw new Error("Stream not found")),
      pipelineConfigs
        .getOrElse("industry-code", throw new Error("Stream not found")),
      withReplace = true
    )
  }

  def generateRecordsIntoIndustryCode(numRecord: Int): Unit = {
    val session = sessionManager.get
    session
      .tableFunction(TableFunction("GENERATE_INDUSTRIES"), lit(numRecord))
      .write
      .mode(SaveMode.Append)
      .saveAsTable(
        pipelineConfigs.getOrElse(
          "industry-code",
          throw new Error("Stream not found")
        )
      )
  }

  def cleanWriteStreamToTableIndustryCodeFirst2(): Unit = {
    val session = sessionManager.get
    val industryCodeStreamName = pipelineConfigs
      .getOrElse("industry-code-stream", throw new Error("Stream not found"))
    val industryCodeNameFirst2 = pipelineConfigs
      .getOrElse("industry-code-first2", throw new Error("Table not found"))
    val industryCodeStreamDf = session.table(industryCodeStreamName)
    industryCodeStreamDf
      .select(
        substring(
          upper(col("districtCode")),
          lit(0),
          lit(2)
        ).as("districtCodeFirst2"),
        col("districtCode"),
        col("departmentCode"),
        col("sizeInSquareMeters")
      )
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(industryCodeNameFirst2)
  }

  def generateRecordsIntoEmployeeCode(numRecord: Int): Unit = {
    val session = sessionManager.get
    session
      .tableFunction(TableFunction("GENERATE_EMPLOYEES"), lit(numRecord))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(
        pipelineConfigs.getOrElse(
          "employee-code",
          throw new Error("Stream not found")
        )
      )
  }

  def industryStreamEmployee(): Unit = {
    val session = sessionManager.get
    val employeeTableName = pipelineConfigs.getOrElse(
      "employee-code",
      throw new Error("Table not found")
    )
    val industryCodeStreamName = pipelineConfigs.getOrElse(
      "industry-code-stream",
      throw new Error("Stream not found")
    )
    val destinationEmployeeIndustry = pipelineConfigs.getOrElse(
      "industry-employee",
      throw new Error("Table not found")
    )

    val employeeDf = session.table(employeeTableName)
    val industryCodeStreamDf = session.table(industryCodeStreamName)
    employeeDf
      .join(
        industryCodeStreamDf,
        employeeDf("districtCodeFirst2") === substring(
          upper(industryCodeStreamDf("districtCode")),
          lit(0),
          lit(2)
        )
      )
      .select(
        employeeDf("*"),
        col("districtCode"),
        col("departmentCode"),
        col("sizeInSquareMeters")
      )
      .write
      .mode(SaveMode.Append)
      .saveAsTable(destinationEmployeeIndustry)
  }
}
