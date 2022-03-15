package com.griddynamics.rest.servlet

import org.eclipse.jetty.http.HttpHeader
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.ScalatraServlet
import org.scalatra.json.JacksonJsonSupport

trait BaseServlet extends ScalatraServlet with JacksonJsonSupport {
  private final val CACHE_CONTROL = "Cache-Control"
  private final val NO_CACHE = "must-revalidate,no-cache,no-store"
  private final val ACCESS_CONTROL_ALLOW_CREDENTIALS =
    "Access-Control-Allow-Credentials"
  private final val TRUE = "true"
  private final val CONTENT_TYPE = "text/plain"

  protected final val ORIGIN = "Origin"
  protected final val ACCESS_CONTROL_ALLOW_ORIGIN =
    "Access-Control-Allow-Origin"
  protected val header: Map[String, String] = Map(
    CACHE_CONTROL -> NO_CACHE,
    ACCESS_CONTROL_ALLOW_CREDENTIALS -> TRUE,
    HttpHeader.CONTENT_TYPE.asString() -> CONTENT_TYPE
  )
  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

}
