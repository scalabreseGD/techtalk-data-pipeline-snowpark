package com.griddynamics.pipeline1

import com.griddynamics.common.sessionManager
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
object Main {
  def main(args: Array[String]): Unit = {
    val session = sessionManager.get
    session.table("SESSION").select(col("VISITNUMBER")).show()
  }

}
