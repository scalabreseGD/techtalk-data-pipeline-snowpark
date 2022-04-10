package com.snowflake.snowpark

import com.snowflake.snowpark.functions.{
  col,
  get_ignore_case,
  lit,
  parse_json,
  to_object
}
import com.snowflake.snowpark.internal.analyzer.SnowflakePlan
import com.snowflake.snowpark.types.{
  ArrayType,
  AtomicType,
  StructField,
  StructType
}

object Implicits {
  implicit class WithCastDataFrame(df: DataFrame) {
    def jsonToFields(
        dataType: StructType,
        columnName: String = "$1"
    ): DataFrame = {

      def getFieldAsColumn(field: StructField): Column = {
        get_ignore_case(
          col(columnName),
          lit(field.name)
        ) cast field.dataType as field.name
      }
      val columns: Array[Column] = dataType.fields.map(getFieldAsColumn)
      df.select(columns)
    }
    def jsonArrayToExplodedFields(
        dataType: ArrayType,
        columnName: String = "$1"
    ): DataFrame = {
      dataType.elementType match {
        case structType: StructType =>
          df.flatten(col(columnName)).jsonToFields(structType, "value")
        case anyType: AtomicType =>
          df.flatten(col(columnName))
            .select(col("value") cast anyType as "value")
      }
    }
  }
}
