package com.griddynamics.rest.servlet

import com.griddynamics.common.ConfigUtils.servlets
import com.griddynamics.common.udfs.generateEmployees
import com.griddynamics.rest.services.{Order, Payment, Rating, Restaurant}
import org.json4s.jackson.Serialization.write
import org.scalatra.ActionResult

class PipelineServlet extends BaseServlet {
  get(
    s"${servlets.generator.endpoints
      .getOrElse("generate-orders", throw new Error("Endpoint missing"))}/:numRecords"
  ) {
    val numRecords: Int = params("numRecords").toInt
    ActionResult(
      200,
      Order.generate(numRecords),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }

  get(
    s"${servlets.generator.endpoints
      .getOrElse("generate-payments", throw new Error("Endpoint missing"))}/:numRecords"
  ) {
    val numRecords: Int = params("numRecords").toInt
    ActionResult(
      200,
      Payment.generate(numRecords),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }

  get(
    s"${servlets.generator.endpoints
      .getOrElse("generate-ratings", throw new Error("Endpoint missing"))}/:numRecords"
  ) {
    val numRecords: Int = params("numRecords").toInt
    ActionResult(
      200,
      Rating.generate(numRecords),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }

  get(
    s"${servlets.generator.endpoints
      .getOrElse("generate-restaurants", throw new Error("Endpoint missing"))}/:numRecords"
  ) {
    val numRecords: Int = params("numRecords").toInt
    ActionResult(
      200,
      Restaurant.generate(numRecords),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }
}
