package com.griddynamics.common.rest_beans

import java.time.LocalDate
import java.util.UUID

case class Payment(
    paymentCode: String,
    paymentType: String,
    paymentDate: String,
    orderCode: String,
    amount: Double
)
object Payment extends Generator[Payment] {
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
      LocalDate.of(1970, 1, 1),
      LocalDate.now()
    ).toString,
    Order.generateOrderCode(),
    generateRandomDoublePrecisionTwo()
  )

  override def configsKey: String = "payments"
}