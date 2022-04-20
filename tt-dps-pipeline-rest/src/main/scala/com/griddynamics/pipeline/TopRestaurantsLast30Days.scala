package com.griddynamics.pipeline

import com.griddynamics.common.configs.ConfigUtils.pipelineConfigs
import com.griddynamics.common.pipeline.Operation
import com.snowflake.snowpark.Session
import com.snowflake.snowpark.functions.{
  avg,
  col,
  dateadd,
  lit,
  round,
  sysdate,
  to_date
}

object TopRestaurantsLast30Days {
  private val bestRatingRestaurant30days =
    pipelineConfigs.demo.tables.get("best_rating_restaurant_30days").orNull
  private val ratingsTableName =
    pipelineConfigs.demo.tables.get("rating").orNull
  private val restaurantTableName =
    pipelineConfigs.demo.tables.get("restaurant").orNull
  private def topRestaurantsLast30Days(session: Session): Unit = {
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
  def apply()(implicit session: Session): Operation = Operation(
    name = "topRestaurantsLast30Days",
    operation = topRestaurantsLast30Days
  )
}
