package com.griddynamics

import com.griddynamics.common.pipeline.{DAG, Operation}
import com.griddynamics.pipeline._
object SamplePipeline extends App {
  import com.griddynamics.common.Implicits.session
  val ingestAndOverwriteRestaurantWithStage =
    IngestAndOverwriteRestaurantWithStage()
  val ingestPaymentsStreamFromStage = IngestPaymentsStreamFromStage()
  val ingestRatingsFromRawToFlat = IngestRatingsFromRawToFlat()
  val ingestOrdersFromRawToFlat = IngestOrdersFromRawToFlat()
  val identifyOrderWithPaymentMoreThanPrice =
    IdentifyOrderWithPaymentMoreThanPrice()

  val paidWithAmexRatingGt50 = PaidWithAmexRatingGt50()

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
    root >> Seq(
      ingestAndOverwriteRestaurantWithStage >> last3,
      ingestPaymentsStreamFromStage >> last3,
      ingestRatingsFromRawToFlat >> last3,
      ingestOrdersFromRawToFlat >> last3
    )
  })
  dag.evaluate()
}
