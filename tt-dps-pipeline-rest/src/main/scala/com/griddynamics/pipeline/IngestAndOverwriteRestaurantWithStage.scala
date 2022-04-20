package com.griddynamics.pipeline

import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.configs.ConfigUtils.{pipelineConfigs, servlets}
import com.griddynamics.common.pipeline.Operation
import com.griddynamics.common.rest_beans.Restaurant
import com.griddynamics.pipeline.utils.stageRestCallFromLocal
import com.snowflake.snowpark.Implicits.WithCastDataFrame
import com.snowflake.snowpark.functions.{col, parse_json}
import com.snowflake.snowpark.{DataFrame, SaveMode, Session}

object IngestAndOverwriteRestaurantWithStage {
  private val restaurantTableName =
    pipelineConfigs.demo.tables.get("restaurant").orNull
  private def ingestAndOverwriteRestaurantWithStage(
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

  def apply(numRecords:Int)(implicit session: Session): Operation = Operation(
    name = "ingestAndOverwriteRestaurantWithStage",
    operation = ingestAndOverwriteRestaurantWithStage,
    parameters = Seq(("numRecords", numRecords))
  )
}
