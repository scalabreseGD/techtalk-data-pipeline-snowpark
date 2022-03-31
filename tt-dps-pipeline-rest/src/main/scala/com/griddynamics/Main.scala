package com.griddynamics

import com.griddynamics.common.ConfigUtils.{pipelineConfigs, servlets}
import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.SnowflakeUtils.StreamSourceMode
import com.griddynamics.common.Types.{Employee, IndustryCode}
import com.griddynamics.pipeline.utils.HttpClientUtils
import com.snowflake.snowpark.functions.{col, concat, lit, parse_json}
import com.snowflake.snowpark.types.{DoubleType, StringType}
import com.snowflake.snowpark.{DataFrame, SaveMode, Session}

object Main {

  private val filePath = s"employees-rest/${System.currentTimeMillis()}"
  private val employeeStageName = "employees_rest"
  private val employeeStreamName = pipelineConfigs.getOrElse(
    "employee-stream",
    throw new Error("Stream Name not present in configs")
  )
  private val employeeTableName = pipelineConfigs.getOrElse(
    "employee",
    throw new Error("Table not present in configs")
  )
  private val industryCodeTableName = pipelineConfigs.getOrElse(
    "industry-code",
    throw new Error("Table not present in configs")
  )

  private val industryStreamName = pipelineConfigs.getOrElse(
    "industry-stream",
    throw new Error("Stream Name not present in configs")
  )

  def industryRestToIndustryTableStream(): Unit = {
    val session: Session = sessionManager.get
    val industryGeneratorUrl: String =
      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
        .getOrElse("generate-industries", throw new Error("Endpoint missing"))}/{{numRecords}}"
    val industries: Array[IndustryCode] =
      HttpClientUtils.performGet[Array[IndustryCode]](
        url = industryGeneratorUrl,
        Map("numRecords" -> 10)
      )
    session
      .createDataFrame(industries)
      .write
      .mode(SaveMode.Append)
      .saveAsTable(industryCodeTableName)

    SnowflakeUtils.createStreamOnObjectType(
      industryStreamName,
      sourceObjectName = industryCodeTableName,
      withReplace = true,
      sourceObjectType = StreamSourceMode.Table
    )
  }

  private def fromEmployeeFileToEmployeeDf(): Unit = {
    val session = sessionManager.get
    val streamContent = session
      .table(employeeStreamName)
      .select(concat(lit(s"@$employeeStageName/"), col("relative_path")))
      .cacheResult()
    val filesToRead: Option[DataFrame] = streamContent
      .collect()
      .map(_.getString(0))
      .map(session.read.json)
      .reduceOption((first: DataFrame, second: DataFrame) => first union second)
    filesToRead.foreach { df =>
      df.select(parse_json(col("*")).as("full"))
        .flatten(col("full"))
        .select(
          col("VALUE")(
            "districtCodeFirst2"
          ) cast StringType as "districtCodeFirst2",
          col("VALUE")("name") cast StringType as "name",
          col("VALUE")("surname") cast StringType as "surname"
        )
        .write
        .mode(SaveMode.Append)
        .saveAsTable(employeeTableName)
    }
  }
  def employeeRestToStageStream(): Unit = {
    val employeeGeneratorUrl =
      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
        .getOrElse("generate-employees", throw new Error("Endpoint missing"))}"
    val employeePath = HttpClientUtils.performGetAndWrite(
      s"$employeeGeneratorUrl/{{numRecords}}",
      Map("numRecords" -> 10),
      filePath
    )
    SnowflakeUtils.createStage(
      employeeStageName,
      orReplace = true,
      ifNotExists = false
    )
    SnowflakeUtils.createStreamOnObjectType(
      employeeStreamName,
      employeeStageName,
      withReplace = true,
      sourceObjectType = StreamSourceMode.Stage
    )

    SnowflakeUtils.stageLocalPath(
      employeeStageName,
      employeePath,
      filePath,
      orReplace = false,
      ifNotExists = true
    )
    fromEmployeeFileToEmployeeDf()
  }

  def parseEmployeeStageStreamToEmployeeStream(): Unit = {
    val session = sessionManager.get
    val streamContent = session
      .table(employeeStreamName)
      .select(concat(lit(s"@$employeeStageName/"), col("relative_path")))
      .cacheResult()
    val filesToRead: Option[DataFrame] = streamContent
      .collect()
      .map(_.getString(0))
      .map(session.read.json)
      .reduceOption((first: DataFrame, second: DataFrame) => first union second)
  }

  def main(args: Array[String]): Unit = {
    industryRestToIndustryTableStream()
    employeeRestToStageStream()
    parseEmployeeStageStreamToEmployeeStream()

//    val session = sessionManager.get
//
//    val streamContent = session
//      .table(industryStreamName)
//      .select(concat(lit(s"@$stageName/"), col("relative_path")))
//      .cacheResult()
//    val filesToRead: Option[DataFrame] = streamContent
//      .collect()
//      .map(_.getString(0))
//      .map(session.read.json)
//      .reduceOption((first: DataFrame, second: DataFrame) => first union second)
//
//    filesToRead.foreach { df =>
//      df.select(parse_json(col("*")).as("full"))
//        .flatten(col("full"))
//        .select(
//          col("VALUE")("departmentCode") cast StringType as "departmentCode",
//          col("VALUE")("districtCode") cast StringType as "districtCode",
//          col("VALUE")(
//            "sizeInSquareMeters"
//          ) cast DoubleType as "sizeInSquareMeters"
//        )
//        .show(10)
//    }
  }

}
