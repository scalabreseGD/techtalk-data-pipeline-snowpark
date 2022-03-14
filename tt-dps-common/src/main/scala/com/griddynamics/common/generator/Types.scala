package com.griddynamics.common.generator

import com.snowflake.snowpark.Row
import com.snowflake.snowpark.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}

object Types {

  trait Rowable extends Product {
    def asRow: Row = Row.fromArray(this.productIterator.toArray)
  }

  case class IndustryCode(
      districtCode: String,
      departmentCode: String,
      sizeInSquareMeters: Double
  ) extends Rowable

  object IndustryCode {
    def schema: StructType = StructType(
      StructField(
        name = "districtCode",
        dataType = StringType,
        nullable = true
      ),
      StructField(
        name = "departmentCode",
        dataType = StringType,
        nullable = true
      ),
      StructField(
        name = "sizeInSquareMeters",
        dataType = DoubleType,
        nullable = true
      )
    )
  }

  case class Employee(name: String, surname: String, districtCodeFirst2: String)
      extends Rowable
  object Employee {
    def schema: StructType = StructType(
      StructField(
        name = "name",
        dataType = StringType,
        nullable = true
      ),
      StructField(
        name = "surname",
        dataType = StringType,
        nullable = true
      ),
      StructField(
        name = "districtCodeFirst2",
        dataType = StringType,
        nullable = true
      )
    )
  }
}
