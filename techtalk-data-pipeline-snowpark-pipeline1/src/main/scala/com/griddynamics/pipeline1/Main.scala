package com.griddynamics.pipeline1

import com.griddynamics.common.sessionManager
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions._
object Main {
  def main(args: Array[String]): Unit = {
    val session = sessionManager.get
    val df:DataFrame = session.table("SESSION")
    df.select(col("VISITNUMBER")).show(10)
  }

}
