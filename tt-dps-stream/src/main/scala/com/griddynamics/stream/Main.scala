package com.griddynamics.stream

import com.griddynamics.common.{SnowflakeUtils, pipelineConfigs, sessionManager}
object Main {

  def main(args: Array[String]): Unit = {
    SnowflakeUtils.createStreamOnTable(
      pipelineConfigs
        .getOrElse("industry-code-stream", throw new Error("Stream not found")),
      pipelineConfigs
        .getOrElse("industry-code", throw new Error("Stream not found")),
      withReplace = true,
      showInitialRows = true
    )
  }
}
