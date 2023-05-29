"""
As a Data Engineer at a digital marketing agency, your team uses an in-house analytics tool that tracks user activity across various campaigns run by the company.
Each user interaction, whether a click or a view, is registered as an event. The collected data, stored in a JSON file named data.json, contains information about the date of the event (event_date) and the count of events that happened on that date (event_count).

The company wants to understand the total number of user interactions that occurred each day to identify trends in user engagement.
As such, your task is to analyze this data and prepare a summary report.
Your report should include the following information:
- The date of the events (event_date).
- The total number of events that occurred on each date (total_events).
The output should be sorted in descending order based on the total number of events, and the results should be saved in a CSV file named output.csv.
"""


#def data_aggregation():
#    pass

from pyspark.sql import *
from pyspark.sql.functions import col, sum
from pyspark.sql import DataFrame
import os

def df_transform_date(df: DataFrame)->DataFrame:
       return df.groupBy("event_date").agg(sum("event_count").alias("total_events"))

def write_df(df: DataFrame)->DataFrameWriter:
    df.repartition(1).write.format("parquet").mode("overwrite").save("02_data_aggregation\output.csv")

def print_schema(df: DataFrame)->DataFrame:
    return df.printSchema()

def cast_types(df: DataFrame, schema: dict)->DataFrame:
    for k,v in schema.items():
        df = df.withColumn(k,col(k).cast(v))
    return df
     

def read_data(spark, filename: str, schema)-> DataFrame:
    df = spark.read.option("multiline","true") \
                    .format("json")\
                    .schema(schema) \
                    .load(filename)
    return df


def data_aggregation(spark, filename, schema, filter_column=None):
   
   df = read_data(spark, filename, schema)   
   dict_out = {"event_date": "date"}                    
   df_cast = cast_types(df, dict_out)                 
   df_date = df_transform_date(df_cast)
    
   write_df(df_date) 
   return df_date
