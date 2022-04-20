package com.griddynamics.pipeline

import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.configs.ConfigUtils.pipelineConfigs
import com.griddynamics.common.pipeline.Operation
import com.snowflake.snowpark.functions.{col, lit}
import com.snowflake.snowpark.{SaveMode, Session}

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
  def apply()(implicit session: Session): Operation = Operation(
    name = "paidWithAmexRatingGt50",
    operation = paidWithAmexRatingGt50
  )
}
