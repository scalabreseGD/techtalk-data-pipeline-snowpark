package com.griddynamics.pipeline

import com.griddynamics.common.{SessionManager, SnowflakeUtils}
import com.griddynamics.common.SnowflakeUtils.StreamSourceMode
import com.griddynamics.common.configs.ConfigUtils.{pipelineConfigs, servlets}
import com.griddynamics.common.pipeline.Operation
import com.griddynamics.common.rest_beans.Rating
import com.griddynamics.pipeline.utils.HttpClientUtils
import com.snowflake.snowpark.Implicits.WithCastDataFrame
import com.snowflake.snowpark.functions.{col, parse_json}
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import com.snowflake.snowpark.{DataFrame, Row, SaveMode, Session}

import scala.util.{Failure, Success, Try}

object IngestRatingsFromRawToFlat {
  private val ratingsTableName =
    pipelineConfigs.demo.tables.get("rating").orNull
  def ingestRatingsFromRawToFlat(
      session: Session,
      params: Seq[(String, Any)]
  ): Unit = {
    val numRecords = params match {
      case ("numRecords", numRecords) :: _ => numRecords
    }
    val ratingUrl =
      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
        .getOrElse("generate-ratings", throw new Error("Endpoint missing"))}/{{numRecords}}"

    val ratingRawTableName =
      pipelineConfigs.demo.tables.get("rating_raw").orNull

    val ratingRawStreamName =
      pipelineConfigs.demo.streams.get("rating_raw").orNull

    val ratings: String = HttpClientUtils
      .performGetJson(ratingUrl, Map("numRecords" -> numRecords))

    session
      .createDataFrame(
        Array(Row(ratings)),
        StructType(
          StructField(
            name = "RESPONSE",
            dataType = StringType,
            nullable = false
          )
        )
      )
      .select(parse_json(col("response")) as "response")
      .write
      .mode(SaveMode.Append)
      .saveAsTable(ratingRawTableName)

    SnowflakeUtils.createStreamOnObjectType(
      ratingRawStreamName,
      ratingRawTableName,
      ifNotExists = true,
      showInitialRows = true,
      sourceObjectType = StreamSourceMode.Table
    )(session)
    SnowflakeUtils.waitStreamsRefresh()

    SnowflakeUtils.executeInTransaction(snowflakeSession => {
      val flattenRatingJson: DataFrame = snowflakeSession
        .table(ratingRawStreamName)
        .jsonArrayToExplodedFields(Rating.schema, "response")

      Try {
        val df = snowflakeSession.table(ratingsTableName)
        df.count()
        df
      } match {
        case Success(destination) =>
          val mergeResult = destination
            .merge(
              flattenRatingJson,
              (destination("customerEmail") === flattenRatingJson(
                "customerEmail"
              )) and (destination("restaurantCode") === flattenRatingJson(
                "restaurantCode"
              ))
            )
            .whenMatched
            .update(
              Map(
                destination("ratingInPercentage") -> flattenRatingJson(
                  "ratingInPercentage"
                ),
                destination("dateOfRate") -> flattenRatingJson(
                  "dateOfRate"
                )
              )
            )
            .whenNotMatched
            .insert(
              destination.schema.fields
                .map(field =>
                  (destination(field.name), flattenRatingJson(field.name))
                )
                .toMap
            )
            .collect()
          println(
            s"\nROW INSERTED = ${mergeResult.rowsInserted} | ROW UPDATED = ${mergeResult.rowsUpdated}\n"
          )
        case Failure(_) =>
          flattenRatingJson.write
            .mode(SaveMode.Overwrite)
            .saveAsTable(ratingsTableName)
      }
    })(session)
  }
  def apply(numRecords:Int)(implicit sessionManager: SessionManager): Operation = Operation(
    name = "ingestRatingsFromRawToFlat",
    operation = ingestRatingsFromRawToFlat,
    parameters = Seq(("numRecords", numRecords))
  )
}
