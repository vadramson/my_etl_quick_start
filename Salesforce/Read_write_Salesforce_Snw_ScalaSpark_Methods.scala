import com.sforce.soap.partner.LoginResult
import com.sforce.ws.ConnectorConfig
//import net.snowflake.spark.snowflake.Utils._
import java.util.Calendar
import java.text.SimpleDateFormat
import com.sforce.soap.partner.{PartnerConnection, QueryResult}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}
import com.sforce.soap.partner.sobject.SObject
import java.text.SimpleDateFormat
import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf


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



// Define JVM options
val conf = new SparkConf()
  .set("spark.driver.memory", "8g")
  .set("spark.executor.memory", "4g")
  .set("spark.driver.maxResultSize", "4g")
  .set("spark.jars.packages",
    "net.snowflake:snowflake-jdbc:3.13.3,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.0")

// Create SNW SparkSession with configured JVM options
val spark = SparkSession.builder()
  .appName("snow_Salesforce_App")
  .master("local[*]")  // Specify a local master URL with "*" indicating to use all available cores
  .config(conf)
  .getOrCreate()


// Main Functions

// Write data to Snowflake
def writeSnowflake(df: DataFrame, tableName: String): Unit = {
  try {
    val jdbcOptions = Map(
      "sfURL" -> url,
      "sfUser" -> user,
      "sfPassword" -> password,
      "sfDatabase" -> database,
      "sfSchema" -> schema,
      "sfWarehouse" -> warehouse,
      "sfRole" -> role,
      "dbtable" -> tableName
    )

    df.write
      .format("snowflake")
      .options(jdbcOptions)
      .mode("append")
      .save()
  } catch {
    case e: Exception =>
      println("An error occurred while writing data to Snowflake:")
      e.printStackTrace()
  }
}


// Read data from Snowflake
def readSnowflake(spark: SparkSession, table: String): DataFrame = {
  val sfOptions = Map(
    "sfURL" -> url,
    "sfUser" -> user,
    "sfPassword" -> password,
    "sfDatabase" -> database,
    "sfSchema" -> schema,
    "sfWarehouse" -> warehouse,
    "sfRole" -> role,
    "dbtable" -> table
  )

  try {
    spark.read
      .format("snowflake")
      .options(sfOptions)
      .load()
  } catch {
    case ex: Exception =>
      println(s"Error reading data from Snowflake table '$table': ${ex.getMessage}")
      spark.emptyDataFrame
  }
}


// Connect to Salesforce
def connectToSalesforce(): PartnerConnection = {
  try {
    val config = new com.sforce.ws.ConnectorConfig()
    config.setUsername(username_sf)
    config.setPassword(password_sf + securityToken_sf)
    config.setAuthEndpoint(authEndPoint_sf)
    config.setServiceEndpoint(authEndPoint_sf)
    config.setCompression(true)
    config.setPrettyPrintXml(true)
    new PartnerConnection(config)
  } catch {
    case ex: Exception => {
      println("Error connecting to Salesforce: " + ex.getMessage())
      null
    }
  }
}


// Describe Salesforce Object
def describeSalesforceObject(connection: PartnerConnection, objectName: String): List[String] = {
  val result = connection.describeSObjects(Array(objectName))
  val describeObjectResult = result(0)
  describeObjectResult.getFields.map(_.getName).toList
}


// Query data from Salesforce
def querySalesforce(connection: PartnerConnection, table: String): DataFrame = {
  // Get metadata for the specified object
  val objectMetadata = connection.describeSObject(table)
  val columns = objectMetadata.getFields.map(_.getName) // Get all table columns

  // Build SOQL query with all columns from the object
  val query = s"SELECT ${columns.mkString(",")} FROM $table LIMIT 300" // Remove LIMIT or modify query as see fit

  // Execute the query and build DataFrame
  val result = connection.query(query)
  val records = result.getRecords
  val data = records.map { record =>
    val values = columns.map { column =>
      Option(record.getField(column)).map(_.toString).getOrElse(null)
    }
    Row.fromSeq(values)
  }

  // Create DataFrame schema
  val schema = StructType(columns.map(col => StructField(col, StringType, true)))

  spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
}


// Connecting to Salesforce
val connection = connectToSalesforce()


// Test connection
val obj_desc = describeSalesforceObject(connection, "asset")
println("This should print a discription of the salesforce object - asset\n" + obj_desc)


// Get Salesforce Table(object) and convert into a spark Dataframe
val sf_df = querySalesforce(connection, "asset")
sf_df.cache()


//  Writing Data to snowflake
writeSnowflake(sf_df, "asset_from_salesforce")
sf_df.unpersist()


// Scalling the Above methos

val objects = List("Account", "User", "Event")

objects.foreach(obj => {
  println("Saving... " + obj)
  val sf_data_df = querySalesforce(connection, obj)
  sf_df.cache()
  writeSnowflake(sf_data_df, obj+"_From_Saleforce")
  sf_df.unpersist()
})