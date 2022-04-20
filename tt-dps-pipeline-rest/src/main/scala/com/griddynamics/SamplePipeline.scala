package com.griddynamics

import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.SnowflakeUtils.StreamSourceMode
import com.griddynamics.common.configs.ConfigUtils.{pipelineConfigs, servlets}
import com.griddynamics.common.pipeline.{DAG, Operation}
import com.griddynamics.common.rest_beans.{Order, Payment, Rating, Restaurant}
import com.griddynamics.pipeline.utils.HttpClientUtils
import com.snowflake.snowpark.Implicits.WithCastDataFrame
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import com.snowflake.snowpark.{DataFrame, Row, SaveMode, Session}

import scala.util.{Failure, Success, Try}

class SamplePipeline {
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
  private val amexRatingGt50TableName =
    pipelineConfigs.demo.tables.get("amex_rating_gt_50").orNull
  private val bestRatingRestaurant30days =
    pipelineConfigs.demo.tables.get("best_rating_restaurant_30days").orNull

  /** @param stageName stageName to create and/or to use
    * @param restUrl rest url to invoke
    * @param restParams rest params such as pathparams
    * @param localPath local path where to store the rest call result
    * @return last file staged
    */
  private def stageRestCallFromLocal(
      session: Session,
      stageName: String,
      restUrl: String,
      restParams: Map[String, Any],
      localPath: String
  ): String = {
    SnowflakeUtils.createStage(
      stageName,
      orReplace = false,
      ifNotExists = true
    )(session)
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
    )(session)
    localFile
  }

  def ingestAndOverwriteRestaurantWithStage(
      session: Session,
      params: Seq[(String, Any)]
  ): Unit = {
    val numRecords = params match {
      case ("numRecords", numRecords) :: _ => numRecords
    }
    val restaurantStageName =
      pipelineConfigs.demo.stages.get("restaurant").orNull
    val restaurantStageLocalPath =
      pipelineConfigs.demo.stages.get("restaurant_local_path").orNull
    val restaurantUrl =
      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
        .getOrElse("generate-restaurants", throw new Error("Endpoint missing"))}/{{numRecords}}"

    val localFile = stageRestCallFromLocal(
      session,
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
    })(session)
  }

  def ingestPaymentsStreamFromStage(
      session: Session,
      params: Seq[(String, Any)]
  ): Unit = {
    val numRecords = params match {
      case ("numRecords", numRecords) :: _ => numRecords
    }
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
    )(session)

    SnowflakeUtils.createStreamOnObjectType(
      paymentStageStreamName,
      paymentStageName,
      ifNotExists = true,
      sourceObjectType = StreamSourceMode.Stage
    )(session)

    stageRestCallFromLocal(
      session,
      paymentStageName,
      paymentUrl,
      Map("numRecords" -> numRecords),
      paymentStageLocalPath
    )

    SnowflakeUtils.waitStreamsRefresh(9000)

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

  def ingestOrdersFromRawToFlat(
      session: Session,
      params: Seq[(String, Any)]
  ): Unit = {
    val numRecords = params match {
      case ("numRecords", numRecords) :: _ => numRecords
    }
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
    )(session)
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
    })(session)
  }
  def identifyOrderWithPaymentMoreThanPrice(session: Session): Unit = {
    val paymentStreamName = pipelineConfigs.demo.streams.get("payment").orNull
    val orderStreamName = pipelineConfigs.demo.streams.get("order").orNull
    SnowflakeUtils.createStreamOnObjectType(
      streamName = paymentStreamName,
      sourceObjectName = paymentsTableName,
      sourceObjectType = StreamSourceMode.Table,
      ifNotExists = true,
      showInitialRows = true
    )(session)

    SnowflakeUtils.createStreamOnObjectType(
      streamName = orderStreamName,
      sourceObjectName = ordersTableName,
      sourceObjectType = StreamSourceMode.Table,
      ifNotExists = true,
      showInitialRows = true
    )(session)

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
    })(session)
  }

  def paidWithAmexRatingGt50(session: Session): Unit = {
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

      orderPayments
        .join(restaurantRatings, usingColumn = "restaurantCode")
        .select(
          orderDf("*"),
          amexPaymentDf("paymentType"),
          ratingsGt50Df("ratingInPercentage")
        )
        .write
        .mode(SaveMode.Append)
        .saveAsTable(amexRatingGt50TableName)
    })(session)
  }

  def topRestaurantsLast30Days(session: Session): Unit = {
    val restaurantDF = session.table(restaurantTableName)
    val ratingDf = session.table(ratingsTableName)
    val bestRating = ratingDf
      .where(
        col("dateOfRate")
          .between(
            to_date(dateadd("day", lit(-30), sysdate())),
            to_date(sysdate())
          )
      )
      .groupBy("restaurantCode")
      .agg(avg(col("ratingInPercentage")).as("ratingInPercentage"))
      .select(
        col("restaurantCode"),
        round(col("ratingInPercentage"), lit(2)).as("ratingInPercentage")
      )

    bestRating
      .join(restaurantDF, usingColumn = "restaurantCode")
      .select(col("restaurantName"), col("ratingInPercentage"))
      .sort(col("ratingInPercentage").desc)
      .createOrReplaceView(bestRatingRestaurant30days)

  }

}

object SamplePipeline extends App {
  import com.griddynamics.common.Implicits.session
  val instance = new SamplePipeline()

  val ingestAndOverwriteRestaurantWithStage = Operation(
    name = "ingestAndOverwriteRestaurantWithStage",
    operation = instance.ingestAndOverwriteRestaurantWithStage,
    parameters = Seq(("numRecords", 1000))
  )

  val ingestPaymentsStreamFromStage = Operation(
    name = "ingestPaymentsStreamFromStage",
    operation = instance.ingestPaymentsStreamFromStage,
    parameters = Seq(("numRecords", 1000))
  )

  val ingestRatingsFromRawToFlat = Operation(
    name = "ingestRatingsFromRawToFlat",
    operation = instance.ingestRatingsFromRawToFlat,
    parameters = Seq(("numRecords", 1000))
  )
  val ingestOrdersFromRawToFlat = Operation(
    name = "ingestOrdersFromRawToFlat",
    operation = instance.ingestOrdersFromRawToFlat,
    parameters = Seq(("numRecords", 1000))
  )
  val identifyOrderWithPaymentMoreThanPrice = Operation(
    name = "identifyOrderWithPaymentMoreThanPrice",
    operation = s => instance.identifyOrderWithPaymentMoreThanPrice(s)
  )

  val paidWithAmexRatingGt50 = Operation(
    name = "paidWithAmexRatingGt50",
    operation = instance.paidWithAmexRatingGt50
  )

  val topRestaurantsLast30Days = Operation(
    name = "topRestaurantsLast30Days",
    operation = instance.topRestaurantsLast30Days
  )

  val dag = DAG("sample").withNodeStructure(root => {
    val last3 = Array(
      identifyOrderWithPaymentMoreThanPrice,
      paidWithAmexRatingGt50,
      topRestaurantsLast30Days
    )
    root >> Seq(ingestAndOverwriteRestaurantWithStage >> last3, ingestPaymentsStreamFromStage >> last3, ingestRatingsFromRawToFlat >> last3, ingestOrdersFromRawToFlat >> last3)
  })
  dag.evaluate()
}
