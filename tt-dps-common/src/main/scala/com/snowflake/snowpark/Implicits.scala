package com.snowflake.snowpark

import com.snowflake.snowpark.functions.col
import com.snowflake.snowpark.types.{ArrayType, AtomicType, StructType}

object Implicits {
  implicit class WithCastDataFrame(df: DataFrame) {
    def jsonToFields(
        dataType: StructType,
        columnName: String = "$1"
    ): DataFrame = {
      val columns: Array[Column] = dataType.fields.map(field =>
        col(columnName)(field.name) cast field.dataType as field.name
      )
      df.select(columns)
    }
    def jsonArrayToExplodedFields(dataType: ArrayType, columnName: String = "$1"): DataFrame = {
      dataType.elementType match {
        case structType: StructType =>
          df.flatten(col(columnName)).jsonToFields(structType,"value")
        case anyType: AtomicType =>
          df.flatten(col(columnName)).select(col("value") cast anyType as "value")
      }
    }
  }
}
