import $file.`import_dependencies`

import com.snowflake.snowpark.Session
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters.mapAsScalaMap
import scala.io.Source

val conf: Map[String, Any] = {
  val buffer = Source.fromFile("./resources/conf.yml").bufferedReader()
  mapAsScalaMap(
    new Yaml()
      .load(buffer)
      .asInstanceOf[java.util.Map[String, Any]]
  ).toMap
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

val session:Session = Session.builder.configs(snowflakeConnectionProperties).create
