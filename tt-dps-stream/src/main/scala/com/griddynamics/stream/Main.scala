package com.griddynamics.stream

import com.griddynamics.common.Implicits.sessionManager
import com.griddynamics.common.SnowflakeUtils
import com.griddynamics.common.udfs.generateUDTFs
import com.snowflake.snowpark.Session

object Main {

  def main(args: Array[String]): Unit = {
    implicit val session: Session = sessionManager.get
    generateUDTFs()
    SampleStream.createIndustryCodeStream()
    SampleStream.generateRecordsIntoEmployeeCode(10000)
    SampleStream.generateRecordsIntoIndustryCode(10000)
    SnowflakeUtils.executeInTransaction(session => {
      SampleStream.cleanWriteStreamToTableIndustryCodeFirst2(session)
      SampleStream.industryStreamEmployee(session)
    })

  }
}
