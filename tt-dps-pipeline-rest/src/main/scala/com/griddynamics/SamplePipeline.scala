package com.griddynamics

import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.SnowflakeUtils.StreamSourceMode
import com.griddynamics.common.configs.ConfigUtils.{pipelineConfigs, servlets}
import com.griddynamics.common.rest_beans.{Payment, Restaurant}
import com.griddynamics.pipeline.utils.HttpClientUtils
import com.snowflake.snowpark.{DataFrame, SaveMode}
import com.snowflake.snowpark.functions.{col, concat, lit, parse_json}
import com.snowflake.snowpark.types.{DoubleType, IntegerType, StringType}
import com.snowflake.snowpark.Implicits.WithCastDataFrame

class SamplePipeline {
  private val session = sessionManager.get
  private val restaurantTableName =
    pipelineConfigs.demo.tables.get("restaurant").orNull
  private val paymentsTableName =
    pipelineConfigs.demo.tables.get("payment").orNull

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
      localFile,
      isTemp = true
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
    stageRestCallToLocal(
      paymentStageName,
      paymentUrl,
      Map("numRecords" -> 10),
      paymentStageLocalPath
    )

    SnowflakeUtils.createStreamOnObjectType(
      paymentStreamName,
      paymentStageName,
      ifNotExists = true,
      sourceObjectType = StreamSourceMode.Stage
    )

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
}

object SamplePipeline extends App {

  val instance = new SamplePipeline()

  instance.ingestAndOverwriteRestaurantWithStage()
  instance.ingestPaymentsStreamFromStage()
}
