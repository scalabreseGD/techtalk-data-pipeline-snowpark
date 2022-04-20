package com.griddynamics.stream

import com.griddynamics.common.SnowflakeUtils
import com.snowflake.snowpark.functions.{col, lit, substring, upper}
import com.snowflake.snowpark.{SaveMode, Session, TableFunction}

object SampleStream {

  def createIndustryCodeStream()(implicit
      session: Session
  ): Unit = {
    SnowflakeUtils.createStreamOnObjectType(
      "INDUSTRY_CODE_STREAM",
      "INDUSTRY_CODE",
      withReplace = true
    )
    SnowflakeUtils.createStreamOnObjectType(
      "INDUSTRY_CODE_STREAM",
      "INDUSTRY_CODE",
      withReplace = true
    )
  }

  def generateRecordsIntoIndustryCode(
      numRecord: Int
  )(implicit session: Session): Unit = {
    session
      .tableFunction(TableFunction("GENERATE_INDUSTRIES"), lit(numRecord))
      .write
      .mode(SaveMode.Append)
      .saveAsTable("INDUSTRY_CODE")
  }

  def generateRecordsIntoEmployeeCode(
      numRecord: Int
  )(implicit session: Session): Unit = {
    session
      .tableFunction(TableFunction("GENERATE_EMPLOYEES"), lit(numRecord))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("EMPLOYEE")
  }

  def cleanWriteStreamToTableIndustryCodeFirst2(session: Session): Unit = {
    val industryCodeStreamName = "INDUSTRY_CODE_STREAM"
    val industryCodeNameFirst2 = "INDUSTRY_CODE_FIRST2"
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

  def industryStreamEmployee(session: Session): Unit = {
    val employeeTableName = "EMPLOYEE"
    val industryCodeStreamName = "INDUSTRY_CODE_STREAM"
    val destinationEmployeeIndustry = "EMPLOYEE_INDUSTRY"

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
