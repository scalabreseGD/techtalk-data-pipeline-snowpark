package com.griddynamics.crud

import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.udfs.generateUDTFs

object Main {
  def main(args: Array[String]): Unit = {
    generateUDTFs()
    SampleCrud.insertSampleIndustryCode(100)
    SampleCrud.performUpdate()
    SampleCrud.performMerge()
  }

}
