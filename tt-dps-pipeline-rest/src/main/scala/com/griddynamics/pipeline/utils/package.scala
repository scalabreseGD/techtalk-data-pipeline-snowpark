package com.griddynamics.pipeline

import com.griddynamics.common.SnowflakeUtils
import com.snowflake.snowpark.Session

package object utils {

  /** @param stageName stageName to create and/or to use
    * @param restUrl rest url to invoke
    * @param restParams rest params such as pathparams
    * @param localPath local path where to store the rest call result
    * @return last file staged
    */
  private[pipeline] def stageRestCallFromLocal(
      session: Session,
      stageName: String,
      restUrl: String,
      restParams: Map[String, Any],
      localPath: String
  ): String = {
    SnowflakeUtils.createStage(
      stageName,
      orReplace = false,
      ifNotExists = true
    )(session)
    val localFile = s"$localPath/${System.currentTimeMillis()}"
    val path = HttpClientUtils.performGetAndWrite(
      restUrl,
      params = restParams,
      localFile
    )
    SnowflakeUtils.stageLocalPath(
      stageName,
      path,
      localPath,
      orReplace = false,
      ifNotExists = true
    )(session)
    localFile
  }
}
