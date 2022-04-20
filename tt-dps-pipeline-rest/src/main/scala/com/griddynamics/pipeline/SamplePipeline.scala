package com.griddynamics.pipeline

import com.griddynamics.common.pipeline.{DAG, Pipeline}

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object SamplePipeline extends App {
  import com.griddynamics.common.Implicits.sessionManager
  val ingestAndOverwriteRestaurantWithStage =
    IngestAndOverwriteRestaurantWithStage(100)
  val ingestPaymentsStreamFromStage = IngestPaymentsStreamFromStage(1000)
  val ingestRatingsFromRawToFlat = IngestRatingsFromRawToFlat(1000)
  val ingestOrdersFromRawToFlat = IngestOrdersFromRawToFlat(1000)
  val dqOrderPaidMoreThanPrice =
    DQOrderPaidMoreThanPrice()
  val paidWithAmexRatingGt50 = PaidWithAmexRatingGt50()
  val topRestaurantsLast30Days = TopRestaurantsLast30Days()

  val dag: DAG = DAG("sample").withNodeStructure(root => {
    val last3 = Array(
      dqOrderPaidMoreThanPrice,
      paidWithAmexRatingGt50,
      topRestaurantsLast30Days
    )
    root >> Seq(
      ingestAndOverwriteRestaurantWithStage >> last3,
      ingestPaymentsStreamFromStage >> last3,
      ingestRatingsFromRawToFlat >> last3,
      ingestOrdersFromRawToFlat >> last3
    )
  })

  val pipeline = Pipeline(dag = dag).asContinuous(1 minute)
  pipeline.evaluate()
}
