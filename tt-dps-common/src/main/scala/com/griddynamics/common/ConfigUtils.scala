package com.griddynamics.common

import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.io.Source

object ConfigUtils {

  case class Servlet(
      baseUrl: String,
      basePath: String,
      port: Int,
      endpoints: Map[String, String]
  )
  case class Servlets(generator: Servlet)

  private def mapAsScala(map: java.util.Map[String, Any]): Map[String, Any] = {
    mapAsScalaMap(map)
      .map(set =>
        set._2 match {
          case t: java.util.Map[String, Any] => (set._1, mapAsScala(t))
          case t: java.util.Collection[Any] =>
            (set._1, collectionAsScalaIterable(t).toList)
          case _                             => (set._1, set._2)
        }
      )
      .toMap
  }

  def readYamlFromResource(path:String): Map[String, Any] = {
    val buffer = Source.fromResource(path).bufferedReader()
    val root = mapAsScala(
      new Yaml()
        .load(buffer)
        .asInstanceOf[java.util.Map[String, Any]]
    )
    root
  }

  val conf: Map[String, Any] = {
    readYamlFromResource("conf.yml")
  }

  val pipelineConfigs: Map[String, String] = conf
    .get("pipeline")
    .map(_.asInstanceOf[Map[String, Map[String, String]]])
    .flatMap(_.get("tables"))
    .getOrElse(throw new Error("Pipeline not defined correctly"))

  val servlets: Servlets = {
    conf
      .get("servlets")
      .flatMap { case x: Map[String, Map[String, Any]] =>
        x.get("generator")
      }
      .map { case x: Map[String, Map[String, Any]] =>
        Servlets(
          Servlet(
            x.getOrElse("baseUrl", "").asInstanceOf[String],
            x.getOrElse("basepath", "").asInstanceOf[String],
            x.getOrElse("port", null).asInstanceOf[Int],
            endpoints = x
              .getOrElse("endpoints", Map.empty)
              .asInstanceOf[Map[String, String]]
          )
        )
      }
      .getOrElse(throw new Error("Error while parsing yaml to Servlets"))
  }

  private[common] lazy val snowflakeConnectionProperties: Map[String, Any] =
    Map(
      "URL" -> conf.getOrElse("snowflake-url", throw new Error()),
      "USER" -> conf.getOrElse("snowflake-user", throw new Error()),
      "PASSWORD" -> conf.getOrElse("snowflake-password", throw new Error()),
      "WAREHOUSE" -> conf.getOrElse("snowflake-warehouse", throw new Error()),
      "DB" -> conf.getOrElse("snowflake-db", throw new Error()),
      "SCHEMA" -> conf.getOrElse("snowflake-schema", throw new Error())
    )
}
