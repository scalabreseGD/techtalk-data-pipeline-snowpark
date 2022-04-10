package com.griddynamics

import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.SnowflakeUtils.StreamSourceMode
import com.griddynamics.common.configs.ConfigUtils.{pipelineConfigs, servlets}
import com.griddynamics.common.rest_beans.{Payment, Rating, Restaurant}
import com.griddynamics.pipeline.utils.HttpClientUtils
import com.snowflake.snowpark.Implicits.{
  WithCastDataFrame,
  EmptyDataframeSession
}
import com.snowflake.snowpark.functions.{col, concat, lit, parse_json}
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import com.snowflake.snowpark.{DataFrame, Row, SaveMode}

import scala.util.{Failure, Success, Try}

class SamplePipeline {
  private val session = sessionManager.get
  private val restaurantTableName =
    pipelineConfigs.demo.tables.get("restaurant").orNull
  private val paymentsTableName =
    pipelineConfigs.demo.tables.get("payment").orNull
  private val ratingsTableName =
    pipelineConfigs.demo.tables.get("rating").orNull

  /** @param stageName stageName to create and/or to use
    * @param restUrl rest url to invoke
    * @param restParams rest params such as pathparams
    * @param localPath local path where to store the rest call result
    * @return last file staged
    */
  private def stageRestCallToLocal(
      stageName: String,
      restUrl: String,
      restParams: Map[String, Any],
      localPath: String
  ): String = {
    SnowflakeUtils.createStage(
      stageName,
      orReplace = false,
      ifNotExists = true
    )
    val localFile = s"$localPath/${System.currentTimeMillis()}"
    val path = HttpClientUtils.performGetAndWrite(
      restUrl,
      params = restParams,
      localFile
    )
    SnowflakeUtils.stageLocalPath(
      stageName,
      path,
      localPath,
      orReplace = false,
      ifNotExists = true
    )
    localFile
  }
  def ingestAndOverwriteRestaurantWithStage(): Unit = {
    val restaurantStageName =
      pipelineConfigs.demo.stages.get("restaurant").orNull
    val restaurantStageLocalPath =
      pipelineConfigs.demo.stages.get("restaurant_local_path").orNull
    val restaurantUrl =
      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
        .getOrElse("generate-restaurants", throw new Error("Endpoint missing"))}/{{numRecords}}"

    val localFile = stageRestCallToLocal(
      restaurantStageName,
      restaurantUrl,
      Map("numRecords" -> 10),
      restaurantStageLocalPath
    )

    SnowflakeUtils.executeInTransaction(s => {
      val df = s.read.json(s"@$restaurantStageName/$localFile")
      val extracted: DataFrame = df
        .select(parse_json(col("*")).as("exploded"))
        .jsonArrayToExplodedFields(Restaurant.schema, "exploded")
      extracted.write.mode(SaveMode.Overwrite).saveAsTable(restaurantTableName)
    })
  }

  def ingestPaymentsStreamFromStage(): Unit = {
    val paymentStageName = pipelineConfigs.demo.stages.get("payment").orNull
    val paymentStreamName = pipelineConfigs.demo.streams.get("payment").orNull
    val paymentStageLocalPath =
      pipelineConfigs.demo.stages.get("payment_local_path").orNull
    val paymentUrl =
      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
        .getOrElse("generate-payments", throw new Error("Endpoint missing"))}/{{numRecords}}"

    SnowflakeUtils.createStage(
      paymentStageName,
      orReplace = false,
      ifNotExists = true
    )

    SnowflakeUtils.createStreamOnObjectType(
      paymentStreamName,
      paymentStageName,
      ifNotExists = true,
      sourceObjectType = StreamSourceMode.Stage
    )

    stageRestCallToLocal(
      paymentStageName,
      paymentUrl,
      Map("numRecords" -> 10),
      paymentStageLocalPath
    )

    SnowflakeUtils.waitStreamsRefresh()

    val fileToReadFromStream: Array[String] = session
      .table(paymentStreamName)
      .select(concat(lit(s"@$paymentStageName/"), col("relative_path")))
      .collect()
      .map(_.getString(0))

    val jsonFileExplodedDF: Option[DataFrame] = fileToReadFromStream
      .map(session.read.json)
      .reduceOption((first: DataFrame, second: DataFrame) => first union second)

    jsonFileExplodedDF.foreach(
      _.select(parse_json(col("*")).as("full"))
        .jsonArrayToExplodedFields(Payment.schema, "full")
        .write
        .mode(SaveMode.Append)
        .saveAsTable(paymentsTableName)
    )
  }

  def ingestRatingsFromRawToFlat(): Unit = {
    val ratingUrl =
      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
        .getOrElse("generate-ratings", throw new Error("Endpoint missing"))}/{{numRecords}}"

    val ratingRawTableName =
      pipelineConfigs.demo.tables.get("rating_raw").orNull

    val ratingRawStreamName =
      pipelineConfigs.demo.streams.get("rating_raw").orNull

    val ratings: String = HttpClientUtils
      .performGetJson(ratingUrl, Map("numRecords" -> 10))

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
    )
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
    })
  }
}

object SamplePipeline extends App {

  val instance = new SamplePipeline()

//  instance.ingestAndOverwriteRestaurantWithStage()
//  instance.ingestPaymentsStreamFromStage()
//  instance.ingestRatingsFromRawToFlat()
}
