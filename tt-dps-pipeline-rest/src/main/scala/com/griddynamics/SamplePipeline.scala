package com.griddynamics

import com.griddynamics.common.pipeline.{DAG, Pipeline}
import com.griddynamics.pipeline._

import scala.concurrent.duration._
import scala.language.postfixOps
object SamplePipeline extends App {
  import com.griddynamics.common.Implicits.session
  val ingestAndOverwriteRestaurantWithStage =
    IngestAndOverwriteRestaurantWithStage(100)
  val ingestPaymentsStreamFromStage = IngestPaymentsStreamFromStage(1000)
  val ingestRatingsFromRawToFlat = IngestRatingsFromRawToFlat(1000)
  val ingestOrdersFromRawToFlat = IngestOrdersFromRawToFlat(1000)
  val identifyOrderWithPaymentMoreThanPrice =
    IdentifyOrderWithPaymentMoreThanPrice()
  val paidWithAmexRatingGt50 = PaidWithAmexRatingGt50()
  val topRestaurantsLast30Days = TopRestaurantsLast30Days()

  val dag: DAG = DAG("sample").withNodeStructure(root => {
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

  val pipeline = Pipeline(dag = dag).asContinuous(5 seconds)
  pipeline.evaluate()
}
