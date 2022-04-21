package com.griddynamics.crud

import com.griddynamics.common.Implicits.sessionManager
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.{MergeResult, SaveMode, Session, TableFunction}

object SampleCrud {
  implicit val session: Session = sessionManager.get
  def insertSampleIndustryCode(numRecord: Int): Unit = {
    session
      .tableFunction(TableFunction("GENERATE_INDUSTRIES"), lit(numRecord))
      .dropDuplicates("districtCode")
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("INDUSTRY_CODE")

    session
      .table("INDUSTRY_CODE")
      .where(
        contains(col("districtCode"), lit("L"))
          .or(contains(col("districtCode"), lit("D")))
      )
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("INDUSTRY_CODE_L_OR_D")
  }

  def performUpdate(): Unit = {
    session
      .table("INDUSTRY_CODE_L_OR_D")
      .update(
        assignments =
          Map("sizeInSquareMeters" -> col("sizeInSquareMeters") * lit(1000)),
        condition = startswith(lower(col("districtCode")), lit("d"))
      )
  }

  def performMerge(): Unit = {
    val sourceDf = session.tableFunction(
      TableFunction("GENERATE_INDUSTRIES"),
      lit(10000)
    )

    val target = session
      .table("INDUSTRY_CODE")
    val result: MergeResult = target
      .merge(
        sourceDf,
        substring(sourceDf("districtCode"), lit(0), lit(4))
          === substring(target("districtCode"), lit(0), lit(4))
      )
      .whenNotMatched
      .insert(
        sourceDf.schema.fields
          .map(field => (field.name, sourceDf(field.name)))
          .toMap
      )
      .whenMatched
      .update(
        Map(
          "sizeInSquareMeters" -> sourceDf("sizeInSquareMeters") * lit(100),
          "districtCode" -> upper(sourceDf("districtCode"))
        )
      )
      .collect()
    print(
      s"Rows Inserted ${result.rowsInserted} - Rows Updated ${result.rowsUpdated}"
    )
  }

  def performDelete(): Unit = {
    session
      .table("INDUSTRY_CODE")
      .delete(col("sizeInSquareMeters") > lit(100))
  }

}
