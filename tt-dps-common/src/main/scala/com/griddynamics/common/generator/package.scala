package com.griddynamics.common

import com.griddynamics.common.generator.Types.{Employee, IndustryCode}
import com.snowflake.snowpark.{DataFrame, Session}

import scala.math.BigDecimal.RoundingMode
import scala.util.Random

package object generator {
  private val random: Random = new Random()

  def generateIndustryDataFrame(session: Session, numRecord: Int): DataFrame = {
    val industries = for {
      _ <- 0 to numRecord
    } yield IndustryCode(
      random.alphanumeric.take(5).mkString(""),
      random.alphanumeric.take(5).mkString(""),
      BigDecimal(random.nextDouble() * 100)
        .setScale(2, RoundingMode.CEILING)
        .toDouble
    )
    session
      .createDataFrame(industries)
      .cacheResult()
  }

  def generateEmployeeDataFrame(session: Session, numRecord: Int): DataFrame = {
    val employees = for {
      _ <- 0 to numRecord
    } yield Employee(
      random.alphanumeric.take(5).mkString(""),
      random.alphanumeric.take(5).mkString(""),
      random.alphanumeric.take(2).mkString("").toUpperCase
    )
    session
      .createDataFrame(employees)
      .cacheResult()
  }
}
