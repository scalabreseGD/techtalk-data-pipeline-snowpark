package com.griddynamics.rest

import org.scalatra.ActionResult

class RestServlet extends BaseServlet {
  get("/getIndustries") {
    ActionResult(
      200,
      Map("test" -> "test"),
      header + (ACCESS_CONTROL_ALLOW_ORIGIN -> request.getHeader(ORIGIN))
    )
  }
}
