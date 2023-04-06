#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark import SparkConf
import json


# Load the Snowflake configuration from the JSON file
with open("cred.json") as f:
    config_pg = json.load(f)

# Create a SparkSession
spark_pg = SparkSession.builder \
    .appName("Conn_RemotePg") \
    .config("spark.driver.extraClassPath", "postgresql-42.2.24.jar") \
    .getOrCreate()

# Set the PostgreSQL options
pgOptions = {
    "url": "jdbc:postgresql://"+config_pg["DB_HOST"]+"/"+config_pg["DATABASE"],
    "user": config_pg["USER"],
    "password": config_pg["PASSWORD"],
}

# Read data from PostgreSQL
df_pg = spark_pg.read \
    .format("jdbc") \
    .options(**pgOptions) \
    .options("dbtable", "my_table")\
    .load()

# Show the data
df_pg.show(3)