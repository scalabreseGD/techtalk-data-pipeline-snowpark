package com.griddynamics.common

import com.griddynamics.common.ConfigUtils.snowflakeConnectionProperties

object Implicits {
  implicit lazy val sessionManager: SessionManager = SessionManager(
    snowflakeConnectionProperties.asInstanceOf[Map[String, String]]
  )
}
