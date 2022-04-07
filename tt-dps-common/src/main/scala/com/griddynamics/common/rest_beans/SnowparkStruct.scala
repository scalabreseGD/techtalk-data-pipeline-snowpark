package com.griddynamics.common.rest_beans

import com.snowflake.snowpark.types.DataType

trait SnowparkStruct {
  def schema: DataType
}
