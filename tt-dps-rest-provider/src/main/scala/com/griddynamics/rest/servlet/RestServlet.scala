package com.griddynamics.rest.servlet

import com.griddynamics.common.udfs.{generateEmployees, generateIndustries}
import com.griddynamics.common.ConfigUtils.servlets
import org.json4s.jackson.Serialization.write
import org.scalatra.ActionResult
class RestServlet extends BaseServlet {

  private def getIndustriesJson(numRecords:Int): String = {
    write(generateIndustries(numRecords))
  }

  get(servlets.generator.endpoints.getOrElse("generate-industries", throw new Error("Endpoint missing"))) {
    ActionResult(
      200,
      write(generateIndustries(10)),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }

  get(servlets.generator.endpoints.getOrElse("generate-employees", throw new Error("Endpoint missing"))) {
    ActionResult(
      200,
      write(generateEmployees(10)),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }
}
