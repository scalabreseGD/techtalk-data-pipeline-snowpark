package com.griddynamics.stream

import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.udfs.generateUDTFs

object Main {

  def main(args: Array[String]): Unit = {
    generateUDTFs(sessionManager.get)
    SampleStream.generateRecordsIntoEmployeeCode(5000)
    SampleStream.createIndustryCodeStream()
    SampleStream.generateRecordsIntoIndustryCode(2000)
    SampleStream.cleanWriteStreamToTableIndustryCodeFirst2()
    SampleStream.generateRecordsIntoIndustryCode(2000)
    SampleStream.industryStreamEmployee()
  }
}
