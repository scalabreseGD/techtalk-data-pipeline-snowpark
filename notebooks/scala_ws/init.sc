import $file.`import_dependencies`

import com.snowflake.snowpark.Session
import org.yaml.snakeyaml.Yaml

import java.nio.file.Paths
import scala.collection.JavaConverters.mapAsScalaMap
import scala.collection.mutable
import scala.io.Source

private def mapAsScala(map: mutable.Map[String, Any]): Map[String, Any] = {
  map
    .map(set =>
      set._2 match {
        case t: java.util.Map[String, Any] =>
          (set._1, mapAsScala(mapAsScalaMap(t)))
        case _ => (set._1, set._2)
      }
    )
    .toMap
}

val conf: Map[String, Any] = {
  val path = Paths.get("./resources/conf.yml").toAbsolutePath.toString
  println(path)
  val buffer = Source.fromFile(path).bufferedReader()
  mapAsScala(
    mapAsScalaMap(
      new Yaml()
        .load(buffer)
        .asInstanceOf[java.util.Map[String, Any]]
    )
  )
}

val snowflakeConnectionProperties: Map[String, String] =
  Map(
    "URL" -> conf.getOrElse("snowflake-url", throw new Error()),
    "USER" -> conf.getOrElse("snowflake-user", throw new Error()),
    "PASSWORD" -> conf.getOrElse("snowflake-password", throw new Error()),
    "WAREHOUSE" -> conf.getOrElse("snowflake-warehouse", throw new Error()),
    "DB" -> conf.getOrElse("snowflake-db", throw new Error()),
    "SCHEMA" -> conf.getOrElse("snowflake-schema", throw new Error())
  ).asInstanceOf[Map[String, String]]

val pipelineConfigs: Map[String, String] = conf
  .get("pipeline")
  .map(_.asInstanceOf[Map[String, Map[String, String]]])
  .flatMap(_.get("tables"))
  .getOrElse(throw new Error("Pipeline not defined correctly"))

implicit val session: Session =
  Session.builder.configs(snowflakeConnectionProperties).create
