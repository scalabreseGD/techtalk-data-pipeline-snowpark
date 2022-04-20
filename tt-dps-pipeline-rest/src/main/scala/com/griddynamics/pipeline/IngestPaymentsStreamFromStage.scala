package com.griddynamics.pipeline

import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.SnowflakeUtils.StreamSourceMode
import com.griddynamics.common.configs.ConfigUtils.{pipelineConfigs, servlets}
import com.griddynamics.common.pipeline.Operation
import com.griddynamics.common.rest_beans.Payment
import com.griddynamics.pipeline.utils.stageRestCallFromLocal
import com.snowflake.snowpark.Implicits.WithCastDataFrame
import com.snowflake.snowpark.functions.{col, concat, lit, parse_json}
import com.snowflake.snowpark.{DataFrame, SaveMode, Session}

object IngestPaymentsStreamFromStage {
  private val paymentsTableName =
    pipelineConfigs.demo.tables.get("payment").orNull
  private def ingestPaymentsStreamFromStage(
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
  def apply(numRecords:Int)(implicit session: Session): Operation = Operation(
    name = "ingestPaymentsStreamFromStage",
    operation = ingestPaymentsStreamFromStage,
    parameters = Seq(("numRecords", numRecords))
  )
}
