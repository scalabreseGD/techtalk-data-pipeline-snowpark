package com.griddynamics.pipeline.utils

import org.json4s.native.JsonMethods
import org.json4s.{DefaultFormats, JValue}

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

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

  def performGetJson(url: String, params: Map[String, Any]): String = {
    val getRequest = HttpRequest
      .newBuilder(new URI(parseParametersInUri(url, params)))
      .GET()
      .build()
    val res =
      HttpClient
        .newHttpClient()
        .send(getRequest, BodyHandlers.ofString())
        .body()
    res
  }

  def performGet[ResponseBody](
      url: String,
      params: Map[String, Any]
  )(implicit m: Manifest[ResponseBody]): ResponseBody = {
    val res = performGetJson(url, params)
    val parsed: JValue = JsonMethods.parse(res)
    parsed.extract[ResponseBody]
  }

  private def createFolderIfNotExists(filePath: Path): Unit = {
    def getParentThree(path: Path): Seq[Path] = {
      if (path.getParent eq path.getRoot)
        Seq(path)
      else
        Seq(path) ++ getParentThree(path.getParent)
    }

    getParentThree(filePath.getParent).reverse.foreach { path =>
      if (!Files.exists(path)) Files.createDirectory(path)
    }
  }

  def performGetAndWrite(
      url: String,
      params: Map[String, Any],
      filePath: String,
      isTemp: Boolean = false
  ): String = {
    val result: String = performGetJson(url, params)
    val path = Paths.get(filePath).toAbsolutePath
    createFolderIfNotExists(path)
    Files.write(
      path,
      result.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE,
      StandardOpenOption.APPEND
    )
    if (isTemp) path.toFile.deleteOnExit()
    path.toString
  }
}
