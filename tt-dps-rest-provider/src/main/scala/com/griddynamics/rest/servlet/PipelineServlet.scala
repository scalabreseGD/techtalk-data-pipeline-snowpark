package com.griddynamics.rest.servlet

import com.griddynamics.common.configs.ConfigUtils.servlets
import com.griddynamics.common.rest_beans._
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
