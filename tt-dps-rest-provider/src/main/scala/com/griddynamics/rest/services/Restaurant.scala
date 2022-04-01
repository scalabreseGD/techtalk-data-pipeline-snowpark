package com.griddynamics.rest.services

import scala.util.Random

case class Restaurant(
    restaurantCode: String,
    restaurantName: String,
    peopleCapacity: Int
)
object Restaurant extends Generator[Restaurant] {

  override def configsKey = "restaurant"

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
  private val capacityRange: (Int, Int) = confs
    .get("capacity")
    .map(_.asInstanceOf[Map[String, Int]])
    .map(numberMap =>
      (
        numberMap.getOrElse("from", 0),
        numberMap.getOrElse("to", 0)
      )
    )
    .getOrElse(throw new Error())

  private[services] def generateRestaurantCode(): String = {
    val codeFirstTwo = codeIntervalChars(
      random.nextInt(codeIntervalChars.length)
    )
    val numberSequence =
      randomIntInRange(codeIntervalNumbers._1, codeIntervalNumbers._2)
    s"$codeFirstTwo$numberSequence"
  }

  override def generate(length: Int): Seq[Restaurant] = for {
    _ <- 0 until length
  } yield Restaurant(
    generateRestaurantCode(),
    random.alphanumeric.take(10).mkString,
    randomIntInRange(capacityRange._1, capacityRange._2)
  )
}
