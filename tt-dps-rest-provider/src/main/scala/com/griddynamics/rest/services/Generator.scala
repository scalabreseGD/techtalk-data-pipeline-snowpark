package com.griddynamics.rest.services

import com.griddynamics.common.{ConfigUtils, udfs}
import com.griddynamics.rest.services.Order.random

import java.sql.Date
import java.time.LocalDate
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

trait Generator[T <: Product] {
  private val configs = ConfigUtils.readYamlFromResource("dataset-domain.yml")
  private def fetchConfsByKey(key: String): Map[String, Any] = {
    configs
      .getOrElse(key, throw new Error(s"Key $key not found."))
      .asInstanceOf[Map[String, Any]]
  }

  protected val random: Random = new Random()

  protected def randomIntInRange(from: Int, to: Int): Int =
    from + random.nextInt(to - from + 1)

  protected def randomDatesBetweenInterval(
      from: LocalDate,
      to: LocalDate
  ): Date = udfs.randomDatesBetweenInterval(from, to)

  protected def generateRandomDoublePrecisionTwo(): Double = {
    BigDecimal
      .decimal(random.nextDouble() * 100)
      .setScale(2, RoundingMode.CEILING)
      .toDouble
  }

  def generate(length: Int): Seq[T]
  def configsKey: String
  val confs: Map[String, Any] = fetchConfsByKey(configsKey)
}
