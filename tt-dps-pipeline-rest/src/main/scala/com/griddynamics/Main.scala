package com.griddynamics

import com.griddynamics.common.ConfigUtils.servlets
import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.Types.IndustryCode
import com.griddynamics.pipeline.utils.HttpClientUtils
import com.snowflake.snowpark.SaveMode

object Main {

  def main(args: Array[String]): Unit = {
    val session = sessionManager.get
    val industryGeneratorUrl =
      s"${servlets.generator.baseUrl}:${servlets.generator.port}${servlets.generator.basePath}${servlets.generator.endpoints
        .getOrElse("generate-industries", throw new Error("Endpoint missing"))}"
    val industries = HttpClientUtils.performGet[List[IndustryCode]](
      s"$industryGeneratorUrl/{{numRecords}}",
      Map("numRecords" -> 10)
    )
    session
      .createDataFrame(industries)
      .write
      .mode(SaveMode.Append)
      .saveAsTable("INDUSTRY_CODE")

//    sessionManager.get
  }

}
