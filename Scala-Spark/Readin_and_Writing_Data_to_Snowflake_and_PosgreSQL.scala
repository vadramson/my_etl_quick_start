//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import net.snowflake.spark.snowflake._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


// Define JVM options
val conf = new SparkConf()
  .set("spark.driver.memory", "8g")
  .set("spark.executor.memory", "4g")
  .set("spark.driver.maxResultSize", "4g")
  .set("spark.jars.packages",
    "net.snowflake:snowflake-jdbc:3.13.3,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.0")

// Create SparkSession with configured JVM options
val spark = SparkSession.builder()
  .appName("snow_pg_App")
  .master("local[*]") // Specify a local master URL with "*" indicating to use all available cores
  .config(conf)
  .getOrCreate()


// Create SparkSession with configured JVM options
val spark_pg = SparkSession.builder()
  .appName("pg_App")
  .master("local[*]") // Specify a local master URL with "*" indicating to use all available cores
  .config(conf)
  .getOrCreate()


// Set Error Log Level
spark.sparkContext.setLogLevel("ERROR")
spark_pg.sparkContext.setLogLevel("ERROR")

println("\n\nRead/Write Snowflake All Good!")

val now = LocalDateTime.now()
val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
val start_time = now.format(formatter)


// Define database connection properties
val url = "url"
val user = "user"
val password = "password"
val database = "database"
val schema = "schema"
val role = "role"
val warehouse = "warehouse"
val account = "account"

// PG Cred

val db_host = "db_host"
val user_pg = "user_pg"
val password_pg = "password_pg"
val pg_tab = "pg_tab"
val database = "database"

// Define list of tables to read from
val tables = List("Table1", "Table2", "Table3", "Table4", "Table15")
tables.foreach(tab => {
  println(tab)
  println(tab + "_Hey")
})

println(s"Current Start time is: $start_time")
tables.foreach(tab => {
  println(tab)
  val df = spark.read
    .format("snowflake")
    .option("sfURL", url)
    .option("sfUser", user)
    .option("sfPassword", password)
    .option("sfDatabase", database)
    .option("sfSchema", schema)
    .option("sfWarehouse", warehouse)
    .option("sfRole", role)
    //.option("dbtable", tab) // Retrieve/Load all data in the table
    .option("query", "SELECT * FROM " + tab + " ORDER BY ID DESC LIMIT 1000000 ") // Retrieve Data using custom query
    .load()
  df.cache()

  // Display data from DataFrame
  df.show(2)

  // Write/Save Snowflake's Data into Postgres
  val pg_tab_sv = "written"
  df.write
    .mode("append")
    .format("jdbc")
    .option("url", "jdbc:postgresql://" + db_host + "/" + database)
    .option("user", user_pg)
    .option("password", password_pg)
    .option("dbtable", tab)
    .save()
  println("Data from table " + tab + " saved into Postgres")

  df.unpersist()
})

val end = LocalDateTime.now()
val formatterend = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
val stop_time = end.format(formatterend)
println(s"Current Stop time is: $stop_time")



