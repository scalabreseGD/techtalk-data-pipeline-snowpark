package com.griddynamics.crud

import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.udfs.generateUDTFs
import com.snowflake.snowpark.Session

object Main {
  def main(args: Array[String]): Unit = {
    implicit val session: Session = sessionManager.get
    generateUDTFs()
    SampleCrud.insertSampleIndustryCode(100)
    SampleCrud.performUpdate()
    SampleCrud.performMerge()
    SampleCrud.performDelete()
  }

}
