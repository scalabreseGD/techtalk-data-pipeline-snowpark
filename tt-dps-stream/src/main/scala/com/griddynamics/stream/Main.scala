package com.griddynamics.stream

object Main {

  def main(args: Array[String]): Unit = {
    SampleStream.generateRecordsIntoEmployeeCode(5000)
    SampleStream.createIndustryCodeStream()
    SampleStream.generateRecordsIntoIndustryCode(2000)
    SampleStream.cleanWriteStreamToTable()
    SampleStream.generateRecordsIntoIndustryCode(2000)
    SampleStream.industryStreamEmployee()
  }
}
