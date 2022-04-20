package com.griddynamics.pipeline

import com.griddynamics.common.{SessionManager, SnowflakeUtils}
import com.griddynamics.common.SnowflakeUtils.StreamSourceMode
import com.griddynamics.common.configs.ConfigUtils.{pipelineConfigs, servlets}
import com.griddynamics.common.pipeline.Operation
import com.griddynamics.common.rest_beans.Order
import com.griddynamics.pipeline.utils.HttpClientUtils
import com.snowflake.snowpark.Implicits.WithCastDataFrame
import com.snowflake.snowpark.functions.{col, parse_json}
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import com.snowflake.snowpark.{DataFrame, Row, SaveMode, Session}

import scala.util.{Failure, Success, Try}

object IngestOrdersFromRawToFlat {
  private val ordersTableName =
    pipelineConfigs.demo.tables.get("order").orNull
  private def ingestOrdersFromRawToFlat(
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
  def apply(numRecords:Int)(implicit sessionManager: SessionManager): Operation = Operation(
    name = "ingestOrdersFromRawToFlat",
    operation = ingestOrdersFromRawToFlat,
    parameters = Seq(("numRecords", numRecords))
  )
}
