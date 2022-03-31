package com.griddynamics.common

import com.griddynamics.common.Types.{Employee, IndustryCode}
import com.snowflake.snowpark.Row
import com.snowflake.snowpark.types.StructType
import com.snowflake.snowpark.udtf.UDTF1

import java.sql.Date
import java.time.{LocalDate, ZoneId, ZoneOffset}
import java.time.temporal.ChronoUnit.DAYS
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

package object udfs {

  def generateIndustries(numRecords: Int): Seq[IndustryCode] = {
    val random = new Random()
    for {
      _ <- 0 to numRecords
    } yield IndustryCode(
      random.alphanumeric.take(5).mkString(""),
      random.alphanumeric.take(5).mkString(""),
      BigDecimal(random.nextDouble() * 100)
        .setScale(2, RoundingMode.CEILING)
        .toDouble
    )
  }

  def randomDatesBetweenInterval(from: LocalDate, to: LocalDate):Date = {
    val diff = DAYS.between(from, to)
    val random = new Random()
    val randDate = from.plusDays(random.nextInt(diff.toInt))
    new Date(randDate.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli)
  }
  def generateEmployees(numRecords: Int): Seq[Employee] = {
    val random = new Random()
    for {
      _ <- 0 to numRecords
    } yield Employee(
      random.alphanumeric.take(5).mkString(""),
      random.alphanumeric.take(5).mkString(""),
      random.alphanumeric.take(2).mkString("").toUpperCase,
      randomDatesBetweenInterval(LocalDate.of(1975,1,1), LocalDate.of(2005,1,1)).toString
    )
  }

  private class GenerateIndustriesUDT extends UDTF1[Int] {
    override def process(numRecords: Int): Iterable[Row] = {
      generateIndustries(numRecords) map (_.asRow)
    }

    override def outputSchema(): StructType = IndustryCode.schema

    override def endPartition(): Iterable[Row] = Array.empty[Row]
  }

  private class GenerateEmployeesUDT extends UDTF1[Int] {

    override def process(numRecords: Int): Iterable[Row] =
      generateEmployees(numRecords) map (_.asRow)

    override def outputSchema(): StructType = Employee.schema

    override def endPartition(): Iterable[Row] = Array.empty[Row]
  }

  def generateUDTFs()(implicit sessionManager: SessionManager): Unit = {
    val session = sessionManager.get
    session.udtf.registerTemporary(
      "GENERATE_INDUSTRIES",
      new GenerateIndustriesUDT()
    )
    session.udtf.registerTemporary(
      "GENERATE_EMPLOYEES",
      new GenerateEmployeesUDT()
    )
  }
}
