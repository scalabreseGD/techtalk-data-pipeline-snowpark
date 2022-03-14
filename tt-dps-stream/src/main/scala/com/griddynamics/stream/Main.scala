package com.griddynamics.stream

import com.griddynamics.common.udfs.generateUDTFs
import com.griddynamics.common.sessionManager

object Main {

  def main(args: Array[String]): Unit = {
    generateUDTFs(sessionManager.get)
    SampleStream.generateRecordsIntoEmployeeCode(5000)
    SampleStream.createIndustryCodeStream()
    SampleStream.generateRecordsIntoIndustryCode(2000)
    SampleStream.cleanWriteStreamToTable()
    SampleStream.generateRecordsIntoIndustryCode(2000)
    SampleStream.industryStreamEmployee()
  }
}
