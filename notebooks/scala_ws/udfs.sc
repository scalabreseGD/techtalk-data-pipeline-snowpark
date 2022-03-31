import $file.`import_dependencies`
import $file.`types`
import types.{Employee, IndustryCode}
import com.snowflake.snowpark.Row
import com.snowflake.snowpark.types.StructType
import com.snowflake.snowpark.udtf.UDTF1
import com.snowflake.snowpark.Session

import scala.math.BigDecimal.RoundingMode
import scala.util.Random

private class GenerateIndustriesUDT extends UDTF1[Int] {
  override def process(numRecords: Int): Iterable[Row] = {
    val random = new Random()
    for {
      _ <- 0 to numRecords
    } yield IndustryCode(
      random.alphanumeric.take(5).mkString(""),
      random.alphanumeric.take(5).mkString(""),
      BigDecimal(random.nextDouble() * 100)
        .setScale(2, RoundingMode.CEILING)
        .toDouble
    ).asRow
  }

  override def outputSchema(): StructType = IndustryCode.schema

  override def endPartition(): Iterable[Row] = Array.empty[Row]
}

private class GenerateEmployeesUDT extends UDTF1[Int] {

  override def process(numRecords: Int): Iterable[Row] ={
    val random = new Random()
    for {
      _ <- 0 to numRecords
    } yield Employee(
      random.alphanumeric.take(5).mkString(""),
      random.alphanumeric.take(5).mkString(""),
      random.alphanumeric.take(2).mkString("").toUpperCase
    ).asRow
  }

  override def outputSchema(): StructType = Employee.schema

  override def endPartition(): Iterable[Row] = Array.empty[Row]
}

def generateUDTFs()(implicit session: Session): Unit = {
  session.udtf.registerTemporary(
    "GENERATE_INDUSTRIES",
    new GenerateIndustriesUDT()
  )
  session.udtf.registerTemporary(
    "GENERATE_EMPLOYEES",
    new GenerateEmployeesUDT()
  )
}
