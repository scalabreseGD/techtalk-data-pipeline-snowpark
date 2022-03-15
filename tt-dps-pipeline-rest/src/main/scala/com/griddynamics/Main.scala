package com.griddynamics

import org.json4s.native.JsonMethods
import org.json4s.{DefaultFormats, JValue}

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}

object Main {

  case class User(
      id: Int,
      name: String,
      email: String,
      gender: String,
      status: String
  )
  def main(args: Array[String]): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val get = HttpRequest
      .newBuilder(new URI("https://gorest.co.in/public/v2/users"))
      .GET()
      .build()
    val res = HttpClient.newHttpClient().send(get, BodyHandlers.ofString()).body()
    val parsed: JValue = JsonMethods.parse(res)
    val parsedResult = parsed.extract[List[User]]
    print(parsedResult)
//    sessionManager.get
  }

}
