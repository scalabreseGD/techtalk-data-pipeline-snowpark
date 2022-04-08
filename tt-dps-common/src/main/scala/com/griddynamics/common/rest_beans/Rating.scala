package com.griddynamics.common.rest_beans

import com.snowflake.snowpark.types.{ArrayType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}

import java.time.LocalDate

case class Rating(
    restaurantCode: String,
    ratingInPercentage: Int,
    dateOfRate: String,
    customerEmail: String = null
)

object Rating extends Generator[Rating] with SnowparkStruct {
  override def configsKey: String = "ratings"

  private val ratingInPercentage: (Int, Int) = confs
    .get("value")
    .map(_.asInstanceOf[Map[String, Int]])
    .map(numberMap =>
      (
        numberMap.getOrElse("from", 0),
        numberMap.getOrElse("to", 0)
      )
    )
    .getOrElse(throw new Error())
  override def generate(length: Int): Seq[Rating] = for {
    _ <- 0 until length
  } yield Rating(
    Restaurant.generateRestaurantCode(),
    randomIntInRange(ratingInPercentage._1, ratingInPercentage._2),
    randomDatesBetweenInterval(
      LocalDate.of(1975, 1, 1),
      LocalDate.now()
    ).toString,
    Order.generateCustomerEmail()
  )

  override def schema: ArrayType = ArrayType(StructType(
    StructField(
      name = "restaurantCode",
      dataType = StringType,
      nullable = true
    ),
    StructField(
      name = "ratingInPercentage",
      dataType = IntegerType,
      nullable = true
    ),
    StructField(
      name = "dateOfRate",
      dataType = StringType,
      nullable = true
    ),
    StructField(
      name = "customerEmail",
      dataType = StringType,
      nullable = true
    )
  ))
}
