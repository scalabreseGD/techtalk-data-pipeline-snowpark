package com.griddynamics

import com.griddynamics.common.ConfigUtils.servlets
import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.SnowflakeUtils.StreamSourceMode
import com.griddynamics.pipeline.utils.HttpClientUtils
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions.{col, concat, lit, parse_json}
import com.snowflake.snowpark.types.{DoubleType, StringType}

object Main {

  private val filePath = s"industries-rest/${System.currentTimeMillis()}"
  private val stageName = "industries_rest"
  private val industryStreamName = "INDUSTRY_REST_STREAM"

  def main(args: Array[String]): Unit = {
    val session = sessionManager.get
    val industryGeneratorUrl =
      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
        .getOrElse("generate-industries", throw new Error("Endpoint missing"))}"
//
//    val employeeGeneratorUrl =
//      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
//        .getOrElse("generate-employees", throw new Error("Endpoint missing"))}"
//    val employees = HttpClientUtils.performGet[List[Employee]](
//      s"$employeeGeneratorUrl/{{numRecords}}",
//      Map("numRecords" -> 10)
//    )
//    session
//      .createDataFrame(employees)
//      .write
//      .mode(SaveMode.Append)
//      .saveAsTable("EMPLOYEE_CODE_PIPELINE")
//
    val path = HttpClientUtils.performGetAndWrite(
      s"$industryGeneratorUrl/{{numRecords}}",
      Map("numRecords" -> 10),
      filePath
    )

    SnowflakeUtils.createStage(stageName, orReplace = true, ifNotExists = false)

    SnowflakeUtils.createStreamOnObjectType(
      industryStreamName,
      stageName,
      withReplace = true,
      sourceObjectType = StreamSourceMode.Stage
    )

    SnowflakeUtils.stageLocalPath(
      stageName,
      path,
      filePath,
      orReplace = false,
      ifNotExists = true
    )

    val streamContent = session
      .table(industryStreamName)
      .select(concat(lit(s"@$stageName/"), col("relative_path")))
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
            col("VALUE")("departmentCode") cast StringType as "departmentCode",
            col("VALUE")("districtCode") cast StringType as "districtCode",
            col("VALUE")(
              "sizeInSquareMeters"
            ) cast DoubleType as "sizeInSquareMeters"
          )
          .show(10)
      }
  }

}
