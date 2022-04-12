package com.griddynamics.common.rest_beans

import com.snowflake.snowpark.types.{ArrayType, DataType, DoubleType, StringType, StructField, StructType}

import java.time.LocalDate
import java.util.UUID

case class Payment(
    paymentCode: String,
    paymentType: String,
    paymentDate: String,
    orderCode: String,
    amount: Double
)
object Payment extends Generator[Payment] with SnowparkStruct {
  val paymentTypes: Seq[String] = confs
    .get("types")
    .map(_.asInstanceOf[List[String]])
    .getOrElse(throw new Error())

  override def generate(length: Int): Seq[Payment] = for {
    _ <- 0 until length
  } yield Payment(
    UUID.randomUUID().toString,
    paymentTypes(
      random.nextInt(paymentTypes.length)
    ),
    randomDatesBetweenInterval(
      LocalDate.of(2000, 1, 1),
      LocalDate.now()
    ).toString,
    Order.generateOrderCode(),
    generateRandomDoublePrecisionTwo()
  )

  override def configsKey: String = "payments"

  override def schema: ArrayType = ArrayType(StructType(
    StructField(
      name = "paymentCode",
      dataType = StringType,
      nullable = true
    ),
    StructField(
      name = "paymentType",
      dataType = StringType,
      nullable = true
    ),
    StructField(
      name = "paymentDate",
      dataType = StringType,
      nullable = true
    ),
    StructField(
      name = "orderCode",
      dataType = StringType,
      nullable = true
    ),
    StructField(
      name = "amount",
      dataType = DoubleType,
      nullable = true
    )
  ))
}