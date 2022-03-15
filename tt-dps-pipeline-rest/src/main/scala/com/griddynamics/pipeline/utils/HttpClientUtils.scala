package com.griddynamics.pipeline.utils

import org.json4s.native.JsonMethods
import org.json4s.{DefaultFormats, JValue}

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}

object HttpClientUtils {
  implicit val formats: DefaultFormats.type = DefaultFormats

  private def parseParametersInUri(
      url: String,
      params: Map[String, Any]
  ): String = {
    params.foldLeft(url)((urlTemp, param) =>
      urlTemp.replace(s"{{${param._1}}}", param._2.toString)
    )
  }

  def performGet[ResponseBody](
      url: String,
      params: Map[String, Any]
  )(implicit m: Manifest[ResponseBody]): ResponseBody = {
    val getRequest = HttpRequest
      .newBuilder(new URI(parseParametersInUri(url, params)))
      .GET()
      .build()
    val res =
      HttpClient
        .newHttpClient()
        .send(getRequest, BodyHandlers.ofString())
        .body()
    val parsed: JValue = JsonMethods.parse(res)
    parsed.extract[ResponseBody]
  }
}
