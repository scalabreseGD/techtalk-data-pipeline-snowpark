package com.griddynamics

import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.SnowflakeUtils.StreamSourceMode
import com.griddynamics.common.configs.ConfigUtils.{pipelineConfigs, servlets}
import com.griddynamics.common.rest_beans.{Order, Payment, Rating, Restaurant}
import com.griddynamics.pipeline.utils.HttpClientUtils
import com.snowflake.snowpark.Implicits.WithCastDataFrame
import com.snowflake.snowpark.functions.{col, concat, lit, parse_json, sum}
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import com.snowflake.snowpark.{
  Column,
  DataFrame,
  MatchedClauseBuilder,
  MergeBuilder,
  MergeResult,
  NotMatchedClauseBuilder,
  Row,
  SaveMode,
  Session
}

import scala.util.{Failure, Success, Try}

class SamplePipeline {
  private val session = sessionManager.get
  private val restaurantTableName =
    pipelineConfigs.demo.tables.get("restaurant").orNull
  private val paymentsTableName =
    pipelineConfigs.demo.tables.get("payment").orNull
  private val ratingsTableName =
    pipelineConfigs.demo.tables.get("rating").orNull
  private val ordersTableName =
    pipelineConfigs.demo.tables.get("order").orNull
  private val dqPaymentMoreThanOrderTableName =
    pipelineConfigs.demo.tables.get("dq_payment_more_than_order").orNull

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

  def ingestAndOverwriteRestaurantWithStage(numRecords: Int): Unit = {
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
      Map("numRecords" -> numRecords),
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

  def ingestPaymentsStreamFromStage(numRecords: Int): Unit = {
    val paymentStageName = pipelineConfigs.demo.stages.get("payment").orNull
    val paymentStageStreamName =
      pipelineConfigs.demo.streams.get("payment_stage").orNull
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
      paymentStageStreamName,
      paymentStageName,
      ifNotExists = true,
      sourceObjectType = StreamSourceMode.Stage
    )

    stageRestCallToLocal(
      paymentStageName,
      paymentUrl,
      Map("numRecords" -> numRecords),
      paymentStageLocalPath
    )

    SnowflakeUtils.waitStreamsRefresh()

    val fileToReadFromStream: Array[String] = session
      .table(paymentStageStreamName)
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

  def ingestRatingsFromRawToFlat(numRecords: Int): Unit = {
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

  def ingestOrdersFromRawToFlat(numRecords: Int): Unit = {
    val orderUrl =
      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
        .getOrElse("generate-orders", throw new Error("Endpoint missing"))}/{{numRecords}}"

    val orderRawTableName =
      pipelineConfigs.demo.tables.get("order_raw").orNull

    val orderRawStreamName =
      pipelineConfigs.demo.streams.get("order_raw").orNull

    val ratings: String = HttpClientUtils
      .performGetJson(orderUrl, Map("numRecords" -> numRecords))

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
      .saveAsTable(orderRawTableName)

    SnowflakeUtils.createStreamOnObjectType(
      orderRawStreamName,
      orderRawTableName,
      ifNotExists = true,
      showInitialRows = true,
      sourceObjectType = StreamSourceMode.Table
    )
    SnowflakeUtils.waitStreamsRefresh()
    SnowflakeUtils.executeInTransaction(snowflakeSession => {
      val flattenOrderJson: DataFrame = snowflakeSession
        .table(orderRawStreamName)
        .jsonArrayToExplodedFields(Order.schema, "response")

      Try {
        val df = snowflakeSession.table(ordersTableName)
        df.count()
        df
      } match {
        case Success(destination) =>
          val mergeResult = destination
            .merge(
              flattenOrderJson,
              destination("orderCode") === flattenOrderJson(
                "orderCode"
              )
            )
            .whenNotMatched
            .insert(
              destination.schema.fields
                .map(field =>
                  (destination(field.name), flattenOrderJson(field.name))
                )
                .toMap
            )
            .collect()
          println(
            s"\nROW INSERTED = ${mergeResult.rowsInserted} | ROW UPDATED = ${mergeResult.rowsUpdated}\n"
          )
        case Failure(_) =>
          flattenOrderJson.write
            .mode(SaveMode.Overwrite)
            .saveAsTable(ordersTableName)
      }
    })
  }
  def identifyOrderWithPaymentMoreThanPrice(): Unit = {
    val paymentStreamName = pipelineConfigs.demo.streams.get("payment").orNull
    val orderStreamName = pipelineConfigs.demo.streams.get("order").orNull
    SnowflakeUtils.createStreamOnObjectType(
      streamName = paymentStreamName,
      sourceObjectName = paymentsTableName,
      sourceObjectType = StreamSourceMode.Table,
      ifNotExists = true,
      showInitialRows = true
    )

    SnowflakeUtils.createStreamOnObjectType(
      streamName = orderStreamName,
      sourceObjectName = ordersTableName,
      sourceObjectType = StreamSourceMode.Table,
      ifNotExists = true,
      showInitialRows = true
    )

    SnowflakeUtils.waitStreamsRefresh()

    SnowflakeUtils.executeInTransaction(snowflakeSession => {
      val orderStreamDf = snowflakeSession.table(orderStreamName)
      val paymentStreamDf = snowflakeSession.table(paymentStreamName)

      val totPaidForEachOrder = paymentStreamDf
        .groupBy(col("orderCode"))
        .agg(sum(col("amount")).as("totPaid"))
      orderStreamDf
        .select(col("orderCode"), col("totPrice"))
        .join(totPaidForEachOrder, usingColumn = "orderCode")
        .where(col("totPaid") gt col("totPrice"))
        .select("orderCode", "totPrice", "totPaid")
        .write
        .mode(SaveMode.Append)
        .saveAsTable(dqPaymentMoreThanOrderTableName)
    })
  }

  def paidWithAmexRatingGt50(): Unit = {
    SnowflakeUtils.executeInTransaction(snowparkSession => {
      val orderDf = snowparkSession.table(ordersTableName)
      val amexPaymentDf = snowparkSession
        .table(paymentsTableName)
        .where(col("paymentType") === lit("AMEX"))
      val ratingsGt50Df = snowparkSession
        .table(ratingsTableName)
        .where(col("ratingInPercentage") > lit(50))
      val restaurantDf = snowparkSession.table(restaurantTableName)
      val orderPayments = orderDf
        .join(amexPaymentDf, usingColumn = "orderCode")
        .select(orderDf("*"), amexPaymentDf("paymentType"))

      val restaurantRatings = restaurantDf
        .join(ratingsGt50Df, usingColumn = "restaurantCode")
        .select(
          restaurantDf("restaurantCode"),
          ratingsGt50Df("ratingInPercentage")
        )

      orderPayments.join(restaurantRatings,usingColumn = "restaurantCode")
        .select(
          orderDf("*"),
          amexPaymentDf("paymentType"),
          ratingsGt50Df("ratingInPercentage")
        )
        .show()
    })
  }

}

object SamplePipeline extends App {

  val instance = new SamplePipeline()

  instance.ingestAndOverwriteRestaurantWithStage(1000)
  instance.ingestPaymentsStreamFromStage(2500)
  instance.ingestRatingsFromRawToFlat(2000)
  instance.ingestOrdersFromRawToFlat(2000)

//  instance.identifyOrderWithPaymentMoreThanPrice()
    instance.paidWithAmexRatingGt50()
}
