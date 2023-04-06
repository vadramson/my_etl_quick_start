#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import json
import pandas as pd
from datetime import datetime

# Load the Snowflake configuration from the JSON file
with open("snow_cred.json") as f:
    config = json.load(f)

# Create a SparkSession
conf = SparkConf() \
    .setAppName("Conn_RemoteSnw") \
    .setMaster("local[*]") \
    .set("spark.jars.packages" ,
         "net.snowflake:snowflake-jdbc:3.13.3,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.0") \
    .set("spark.driver.memory" , "8g") \
    .set("spark.executor.memory" , "4g") \
    .set("spark.driver.maxResultSize" , "4g")

sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

# Set the Snowflake options
sfOptions = {
    "sfURL": f"{config['account']}.snowflakecomputing.com" ,
    "sfUser": config['user'] ,
    "sfPassword": config['password'] ,
    "sfDatabase": config['database'] ,
    "sfSchema": config['schema'] ,
    "sfWarehouse": config['warehouse'] ,
    "sfRole": config['role']
}

# Read data from Snowflake
df = spark.read \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("query" , "SELECT TABLE_NAME FROM Tables_to_migrate LIMIT 5") \
    .load()

# Show the data
df.show()
# df = df.toPandas()   # Convert the PySpark dataframe to Pandas dataframe.  *It's just a small dataframe

print(list(df.toPandas().TABLE_NAME))
test_lst = ['Table1', 'Table2', 'Table3', 'Table4', 'Table5']
start = datetime.now()

#for tbl in list(df.toPandas().TABLE_NAME):
for tbl in test_lst:
    print('df --> ' , tbl)

    # Get the data to save
    sfOptions["sfSchema"] = "PUBLIC"
    df_tmp = spark.read \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("query" , "SELECT * FROM " + tbl + " ORDER BY ID DESC LIMIT 10") \
        .load()
    df_tmp.show(2)

    df_tmp.cache()  # Cache the DataFrame in memory

    # Save data to different schema
    sfOptions["sfSchema"] = "some_schema"
    df_tmp.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable" , tbl) \
        .mode("overwrite") \
        .save()
    df_tmp.unpersist() # Remove the DataFrame from memory

stop = datetime.now()
print("Start " , start , "\nStop" , stop)
print('Time diff ', stop - start)

"""
data_collect = df.collect()
# looping thorough each row of the dataframe
for row in data_collect:
    tab = row["TABLE_NAME"]
    print(tab)

df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable" , "tables") \
    .mode("overwrite") \
    .save()
"""
print("saved!")
