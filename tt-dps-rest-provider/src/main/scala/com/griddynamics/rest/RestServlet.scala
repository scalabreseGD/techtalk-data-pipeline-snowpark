package com.griddynamics.rest

import com.griddynamics.common.udfs.{generateEmployees, generateIndustries}
import org.json4s.jackson.Serialization.write
import org.scalatra.ActionResult
class RestServlet extends BaseServlet {

  private def getIndustriesJson(numRecords:Int): String = {
    write(generateIndustries(numRecords))
  }

  get("/getIndustries") {
    ActionResult(
      200,
      write(generateIndustries(10)),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }

  get("/getEmployees") {
    ActionResult(
      200,
      write(generateEmployees(10)),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }
}
