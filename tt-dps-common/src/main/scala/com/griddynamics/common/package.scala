package com.griddynamics

import org.yaml.snakeyaml.Yaml

import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.mutable


package object common {

  val conf: mutable.Map[String, String] = {
    val buffer = Source.fromResource("com/griddynamics/conf.yml").bufferedReader()
    mapAsScalaMap(new Yaml()
      .load(buffer)
      .asInstanceOf[java.util.Map[String, String]])
  }

  private lazy val snowflakeConnectionProperties: Map[String, String] = Map(
    "URL" -> conf.getOrElse("snowflake-url",throw new Error()),
    "USER" -> conf.getOrElse("snowflake-user",throw new Error()),
    "PASSWORD" -> conf.getOrElse("snowflake-password",throw new Error()),
    "WAREHOUSE" -> conf.getOrElse("snowflake-warehouse",throw new Error()),
    "DB" -> conf.getOrElse("snowflake-db",throw new Error()),
    "SCHEMA" -> conf.getOrElse("snowflake-schema",throw new Error())
  )

  implicit lazy val sessionManager: SessionManager = SessionManager(snowflakeConnectionProperties)
}
