package com.griddynamics.stream

import com.griddynamics.common.ConfigUtils.pipelineConfigs
import com.griddynamics.common.{SessionManager, SnowflakeUtils}
import com.snowflake.snowpark.functions.{col, lit, substring, upper}
import com.snowflake.snowpark.{SaveMode, Session, TableFunction}

object SampleStream {

  def createIndustryCodeStream()(implicit sessionManager:SessionManager): Unit = {
    SnowflakeUtils.createStreamOnTable(
      pipelineConfigs
        .getOrElse("industry-code-stream", throw new Error("Stream not found")),
      pipelineConfigs
        .getOrElse("industry-code", throw new Error("Stream not found")),
      withReplace = true
    )
  }

  def generateRecordsIntoIndustryCode(numRecord: Int)(implicit sessionManager:SessionManager): Unit = {
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

  def generateRecordsIntoEmployeeCode(numRecord: Int)(implicit sessionManager:SessionManager): Unit = {
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

  def cleanWriteStreamToTableIndustryCodeFirst2(session:Session): Unit = {
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
      .mode(SaveMode.Append)
      .saveAsTable(industryCodeNameFirst2)
  }

  def industryStreamEmployee(session:Session): Unit = {
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
