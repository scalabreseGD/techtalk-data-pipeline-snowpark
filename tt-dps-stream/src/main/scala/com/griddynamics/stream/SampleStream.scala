package com.griddynamics.stream

import com.griddynamics.common.generator.{
  generateEmployeeDataFrame,
  generateIndustryDataFrame
}
import com.griddynamics.common.{SnowflakeUtils, pipelineConfigs, sessionManager}
import com.snowflake.snowpark.SaveMode
import com.snowflake.snowpark.functions.{col, lit, substring, upper}

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

  def createEmployeeCodeStream(): Unit = {
    SnowflakeUtils.createStreamOnTable(
      pipelineConfigs
        .getOrElse("industry-code-stream", throw new Error("Stream not found")),
      pipelineConfigs
        .getOrElse("employee-code", throw new Error("Stream not found")),
      withReplace = true
    )
  }

  def generateRecordsIntoIndustryCode(numRecord: Int): Unit = {
    SnowflakeUtils.writeToTable(
      dataframeGenerator =
        session => generateIndustryDataFrame(session, numRecord),
      SaveMode.Append,
      tableName = pipelineConfigs.getOrElse(
        "industry-code",
        throw new Error("Stream not found")
      )
    )
  }

  def cleanWriteStreamToTable(): Unit = {
    SnowflakeUtils.writeFromTableToTable(
      sourceTable = pipelineConfigs
        .getOrElse("industry-code-stream", throw new Error("Stream not found")),
      transformer = dataframe =>
        dataframe.select(
          substring(
            upper(col("districtCode")),
            lit(0),
            lit(2)
          ).as("districtCodeFirst2"),
          col("districtCode"),
          col("departmentCode"),
          col("sizeInSquareMeters")
        ),
      destinationTableName = pipelineConfigs
        .getOrElse("industry-code-first2", throw new Error("Table not found")),
      saveMode = SaveMode.Overwrite
    )
  }

  def generateRecordsIntoEmployeeCode(numRecord: Int): Unit = {
    SnowflakeUtils.writeToTable(
      dataframeGenerator =
        session => generateEmployeeDataFrame(session, numRecord),
      SaveMode.Overwrite,
      tableName = pipelineConfigs.getOrElse(
        "employee-code",
        throw new Error("Stream not found")
      )
    )
  }

  def industryStreamEmployee(): Unit = {
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

    val codeFirstTwoUpperColumn = substring(
      upper(col("districtCode")),
      lit(0),
      lit(2)
    )

    SnowflakeUtils.writeFromTableToTable2(
      sourceTablesName = (employeeTableName, industryCodeStreamName),
      destinationTableName = destinationEmployeeIndustry,
      saveMode = SaveMode.Overwrite,
      transformer = (employee, industryStream) => {
        val joined = employee.join(
          industryStream,
          employee("districtCodeFirst2") === codeFirstTwoUpperColumn
        )
        joined.select(
          employee("*"),
          col("districtCode"),
          col("departmentCode"),
          col("sizeInSquareMeters")
        )
      }
    )
  }
}
