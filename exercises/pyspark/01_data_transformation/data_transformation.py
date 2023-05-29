"""
You are a Data Engineer at an online retail company that has a significant number of daily users on its website.
The company captures several types of user interaction data: including user clicks, views, and purchases.

You are given a CSV file named data.csv, which contains this information. The columns in the file are as follows: user_id, timestamp, event_type, and duration.
Your task is to perform an analysis to better understand user behavior on the website.
Specifically, your manager wants to understand the average duration of a ‘click’ event for each user.
This means you need to consider only those events where users have clicked on something on the website.

Finally, your analysis should be presented in the form of a Parquet file named output.parquet that contains two columns: user_id and avg_duration.
The challenge here is to devise the most efficient and accurate solution using PySpark to read, process, and write the data. Please also keep in mind the potential size and scale of the data while designing your solution.
"""

from pyspark.sql import *
from pyspark.sql.functions import col, avg
from pyspark.sql import DataFrame
import os


def filter_df(df: DataFrame, filter_column)->DataFrame:
    return df.where(col("event_type")=="click")

def df_transform_date(df: DataFrame)->DataFrame:
       return df.groupBy("user_id").agg(avg("duration").alias("avg_duration"))

def write_df(df: DataFrame)->DataFrameWriter:
    df.repartition(1).write.format("parquet").mode("overwrite").save("01_data_transformation\output.parquet")

def print_schema(df: DataFrame)->DataFrame:
    return df.printSchema()

def cast_types(df: DataFrame, schema: dict)->DataFrame:
    for k,v in schema.items():
        df = df.withColumn(k,col(k).cast(v))
    return df
     

def read_data(spark, filename: str, schema)-> DataFrame:
    df = spark.read.format("csv")\
                          .option("header", True)\
                          .option("delimiter", ",")\
                          .schema(schema) \
                          .load(filename)
    return df


def data_transformation(spark, filename, filter_column, schema):
   
   df = read_data(spark, filename, schema)                       
                    
   df_click = filter_df(df, "event_type")
   df_date = df_transform_date(df_click)
   dict_out = {"user_id": "int", "avg_duration": "double"}
   df_cast = cast_types(df_date, dict_out)
   
   write_df(df_cast) 
   return df_cast