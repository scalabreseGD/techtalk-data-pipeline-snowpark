package com.griddynamics.rest.services

import java.time.LocalDate

case class Rating(
    restaurantCode: String,
    ratingInPercentage: Int,
    dateOfRate: String,
    customerEmail: String = null
)

object Rating extends Generator[Rating] {
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
    null
  )
}
