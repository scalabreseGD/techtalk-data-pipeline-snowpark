package com.griddynamics.pipeline

import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.SnowflakeUtils.StreamSourceMode
import com.griddynamics.common.configs.ConfigUtils.pipelineConfigs
import com.griddynamics.common.pipeline.Operation
import com.snowflake.snowpark.functions.{col, sum}
import com.snowflake.snowpark.{SaveMode, Session}

object IdentifyOrderWithPaymentMoreThanPrice {
  private val ordersTableName =
    pipelineConfigs.demo.tables.get("order").orNull
  private val paymentsTableName =
    pipelineConfigs.demo.tables.get("payment").orNull
  private val dqPaymentMoreThanOrderTableName =
    pipelineConfigs.demo.tables.get("dq_payment_more_than_order").orNull
  private def identifyOrderWithPaymentMoreThanPrice(session: Session): Unit = {
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
  def apply()(implicit session: Session): Operation = Operation(
    name = "identifyOrderWithPaymentMoreThanPrice",
    operation = identifyOrderWithPaymentMoreThanPrice
  )
}
