package com.griddynamics.rest.servlet

import com.griddynamics.common.ConfigUtils.servlets
import com.griddynamics.common.udfs.{generateEmployees, generateIndustries}
import org.json4s.jackson.Serialization.write
import org.scalatra.ActionResult
class RestServlet extends BaseServlet {

  get(
    s"${servlets.generator.endpoints
      .getOrElse("generate-industries", throw new Error("Endpoint missing"))}/:numRecords"
  ) {
    val numRecords: Int = params("numRecords").toInt
    ActionResult(
      200,
      write(generateIndustries(numRecords)),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }

  get(
    s"${servlets.generator.endpoints
      .getOrElse("generate-employees", throw new Error("Endpoint missing"))}/:numRecords"
  ) {
    val numRecords: Int = params("numRecords").toInt
    ActionResult(
      200,
      write(generateEmployees(numRecords)),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }
}
