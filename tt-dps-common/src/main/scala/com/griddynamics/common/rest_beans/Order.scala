package com.griddynamics.common.rest_beans
import com.snowflake.snowpark.types.{
  ArrayType,
  DataType,
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType
}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class Order(
    orderCode: String,
    customerEmail: String,
    totPrice: Double,
    restaurantCode: String,
    dateOfOrder: String
)
object Order extends Generator[Order] with SnowparkStruct {
  private val codeIntervalChars: List[String] = confs
    .get("code-interval")
    .map(_.asInstanceOf[Map[String, Any]])
    .flatMap(_.get("chars"))
    .map(_.asInstanceOf[Iterable[String]])
    .getOrElse(throw new Error())
    .toList
  private val codeIntervalNumbers: (Int, Int) = confs
    .get("code-interval")
    .map(_.asInstanceOf[Map[String, Any]])
    .flatMap(_.get("numbers"))
    .map(_.asInstanceOf[Map[String, Int]])
    .map(numberMap =>
      (
        numberMap.getOrElse("from", 0),
        numberMap.getOrElse("to", 0)
      )
    )
    .getOrElse(throw new Error())

  private val emailConfs: (Int, Int) = confs
    .get("customerEmail")
    .map(_.asInstanceOf[Map[String, Int]])
    .map(numberMap =>
      (
        numberMap.getOrElse("length", 0),
        numberMap.getOrElse("domainLength", 0)
      )
    )
    .getOrElse(throw new Error())

  private[rest_beans] def generateOrderCode(): String = {
    val codeFirstTwo = codeIntervalChars(
      random.nextInt(codeIntervalChars.length)
    )
    val numberSequence =
      randomIntInRange(codeIntervalNumbers._1, codeIntervalNumbers._2)
    s"$codeFirstTwo$numberSequence"
  }

  private[rest_beans] def generateCustomerEmail(): String = {
    s"${random.alphanumeric.take(emailConfs._1).mkString}@${random.alphanumeric.take(emailConfs._2).mkString}.test"
  }
  override def generate(length: Int): Seq[Order] = for {
    _ <- 0 until length
  } yield Order(
    generateOrderCode(),
    generateCustomerEmail(),
    generateRandomDoublePrecisionTwo(),
    Restaurant.generateRestaurantCode(),
    LocalDateTime
      .now()
      .format(DateTimeFormatter.ISO_DATE)
  )

  override def configsKey: String = "orders"

  override def schema: ArrayType = ArrayType(
    StructType(
      StructField(
        name = "orderCode",
        dataType = StringType,
        nullable = true
      ),
      StructField(
        name = "customerEmail",
        dataType = StringType,
        nullable = true
      ),
      StructField(
        name = "totPrice",
        dataType = DoubleType,
        nullable = true
      ),
      StructField(
        name = "restaurantCode",
        dataType = StringType,
        nullable = true
      ),
      StructField(
        name = "dateOfOrder",
        dataType = StringType,
        nullable = true
      )
    )
  )
}
