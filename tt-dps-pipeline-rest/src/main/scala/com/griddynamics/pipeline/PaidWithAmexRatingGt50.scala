package com.griddynamics.pipeline

import com.griddynamics.common.{SessionManager, SnowflakeUtils}
import com.griddynamics.common.configs.ConfigUtils.pipelineConfigs
import com.griddynamics.common.pipeline.Operation
import com.snowflake.snowpark.functions.{col, lit}
import com.snowflake.snowpark.{SaveMode, Session}
import com.snowflake.snowpark.Implicits.WaitForASyncJobs

import scala.util.{Failure, Success, Try}

object PaidWithAmexRatingGt50 {
  private val amexRatingGt50TableName =
    pipelineConfigs.demo.tables.get("amex_rating_gt_50").orNull
  private val ordersTableName =
    pipelineConfigs.demo.tables.get("order").orNull
  private val paymentsTableName =
    pipelineConfigs.demo.tables.get("payment").orNull
  private val ratingsTableName =
    pipelineConfigs.demo.tables.get("rating").orNull
  private val restaurantTableName =
    pipelineConfigs.demo.tables.get("restaurant").orNull
  private def paidWithAmexRatingGt50(session: Session): Unit = {
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
        .select("paymentType", orderDf.schema.fields.map(f => f.name): _*)

      val restaurantRatings = restaurantDf
        .join(ratingsGt50Df, usingColumn = "restaurantCode")
        .select(
          restaurantDf("restaurantCode"),
          ratingsGt50Df("ratingInPercentage")
        )

      val dqAmexSource = orderPayments
        .join(restaurantRatings, usingColumn = "restaurantCode")
        .select(
          orderDf.schema.fields.map(f => col(f.name)) ++ Seq(
            amexPaymentDf(
              "paymentType"
            ),
            ratingsGt50Df("ratingInPercentage")
          )
        )

      Try {
        val df = session.table(amexRatingGt50TableName)
        df.count()
        df
      } match {
        case Failure(_) =>
          dqAmexSource.write
            .mode(SaveMode.Overwrite)
            .async
            .saveAsTable(amexRatingGt50TableName)
            .addToShutDownHook()
        case Success(destination) =>
          destination
            .merge(
              dqAmexSource,
              dqAmexSource("orderCode") === destination("orderCode")
            )
            .whenNotMatched
            .insert(
              destination.schema.fields
                .map(field =>
                  (destination(field.name), dqAmexSource(field.name))
                )
                .toMap
            )
            .async
            .collect()
            .addToShutDownHook()
      }
    })(session)
  }
  def apply()(implicit sessionManager: SessionManager): Operation = Operation(
    name = "paidWithAmexRatingGt50",
    operation = paidWithAmexRatingGt50
  )
}
