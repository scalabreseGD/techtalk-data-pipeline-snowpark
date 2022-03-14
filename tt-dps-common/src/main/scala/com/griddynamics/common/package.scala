package com.griddynamics

import org.yaml.snakeyaml.Yaml

import scala.annotation.tailrec
import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.mutable

package object common {

  private def mapAsScala[T <: java.util.Map[String, Any]](
      map: T
  ): Map[String, Any] = {
    mapAsScalaMap(map)
      .map(set =>
        set._2 match {
          case t: T => (set._1, mapAsScala(t))
          case _    => (set._1, set._2)
        }
      )
      .toMap
  }

  val conf: Map[String, Any] = {
    val buffer = Source.fromResource("conf.yml").bufferedReader()
    val root = mapAsScala(
      new Yaml()
        .load(buffer)
        .asInstanceOf[java.util.Map[String, Any]]
    )
    root
  }

  val pipelineConfigs: Map[String, String] = conf
    .get("pipeline")
    .map(_.asInstanceOf[Map[String, Map[String, String]]])
    .flatMap(_.get("tables"))
    .getOrElse(throw new Error("Pipeline not defined correctly"))

  private lazy val snowflakeConnectionProperties: Map[String, Any] = Map(
    "URL" -> conf.getOrElse("snowflake-url", throw new Error()),
    "USER" -> conf.getOrElse("snowflake-user", throw new Error()),
    "PASSWORD" -> conf.getOrElse("snowflake-password", throw new Error()),
    "WAREHOUSE" -> conf.getOrElse("snowflake-warehouse", throw new Error()),
    "DB" -> conf.getOrElse("snowflake-db", throw new Error()),
    "SCHEMA" -> conf.getOrElse("snowflake-schema", throw new Error())
  )

  implicit lazy val sessionManager: SessionManager = SessionManager(
    snowflakeConnectionProperties.asInstanceOf[Map[String, String]]
  )
}
