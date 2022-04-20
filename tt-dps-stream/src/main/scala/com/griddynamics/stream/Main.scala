package com.griddynamics.stream

import com.griddynamics.common.Implicits.session
import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.udfs.generateUDTFs

object Main {

  def main(args: Array[String]): Unit = {
    generateUDTFs()
    SampleStream.createIndustryCodeStream()
    SampleStream.generateRecordsIntoEmployeeCode(5000)
    SampleStream.generateRecordsIntoIndustryCode(2000)
    SnowflakeUtils.executeInTransaction(session => {
      SampleStream.cleanWriteStreamToTableIndustryCodeFirst2(session)
      SampleStream.industryStreamEmployee(session)
    })

  }
}
