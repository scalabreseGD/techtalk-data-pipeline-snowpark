package com.griddynamics.crud

import com.griddynamics.common.SnowflakeWriter.JoinCriteria
import com.griddynamics.common.{SnowflakeWriter, sessionManager}
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.{DataFrame, SaveMode, Session}

import scala.math.BigDecimal.RoundingMode
import scala.util.Random

object SampleCrud {

  case class IndustryCode(
      districtCode: String,
      departmentCode: String,
      sizeInSquareMeters: Double
  )

  def generateIndustryDataFrame(session: Session, numRecord: Int): DataFrame = {
    val random = new Random()
    val industries = for {
      _ <- 0 to numRecord
    } yield IndustryCode(
      random.alphanumeric.take(5).mkString(""),
      random.alphanumeric.take(5).mkString(""),
      BigDecimal(random.nextDouble() * 100)
        .setScale(2, RoundingMode.CEILING)
        .toDouble
    )
//    val viewName = "temp_industry_code_" + System.currentTimeMillis()
    session
      .createDataFrame(industries)
      .cacheResult()
  }

  def insertSampleIndustryCode(numRecord: Int): Unit = {

    SnowflakeWriter.writeBronzeLayer(
      session => generateIndustryDataFrame(session, numRecord),
      SaveMode.Overwrite,
      "INDUSTRY_CODE"
    )
    SnowflakeWriter.writeFromTableToTable(
      "INDUSTRY_CODE",
      simonTestDataframe =>
        simonTestDataframe.where(
          contains(col("districtCode"), lit("L"))
            .or(contains(col("districtCode"), lit("D")))
        ),
      "INDUSTRY_CODE_L_OR_D",
      SaveMode.Overwrite
    )

  }

  def performUpdate(): Unit = {
    SnowflakeWriter.update(
      "INDUSTRY_CODE_L_OR_D",
      condition = startswith(lower(col("districtCode")), lit("d")),
      assignments =
        Map("sizeInSquareMeters" -> col("sizeInSquareMeters") * lit(1000))
    )
  }

  def performMerge(): Unit = {
    val sourceDf = generateIndustryDataFrame(sessionManager.get, 1000)
    val firstTwoLectersDistrictCode: JoinCriteria = (source, destination) => {
      substring(source("districtCode"), lit(0), lit(2))
        .equal_to(substring(destination("districtCode"), lit(0), lit(2)))
    }

    SnowflakeWriter.merge(
      "INDUSTRY_CODE",
      sourceDf,
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
