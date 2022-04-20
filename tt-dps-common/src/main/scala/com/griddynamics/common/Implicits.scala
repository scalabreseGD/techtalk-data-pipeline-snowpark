package com.griddynamics.common

import com.griddynamics.common.configs.ConfigUtils.snowflakeConnectionProperties
import com.snowflake.snowpark.Session

object Implicits {
  implicit lazy val session: Session = SessionManager(
    snowflakeConnectionProperties.asInstanceOf[Map[String, String]]
  ).get
}
