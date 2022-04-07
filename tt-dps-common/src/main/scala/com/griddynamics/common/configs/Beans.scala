package com.griddynamics.common.configs

object Beans {
  case class Servlet(
      baseUrl: String,
      basePath: String,
      port: Int,
      endpoints: Map[String, String]
  )
  case class Servlets(generator: Servlet)

  case class Pipeline(
      tables: Map[String, String],
      streams: Map[String, String],
      stages: Map[String, String]
  )
  case class Pipelines(demo: Pipeline)

}
