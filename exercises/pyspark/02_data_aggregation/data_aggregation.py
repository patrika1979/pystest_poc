from pyspark.sql import *
from pyspark.sql.functions import col, sum, desc
from pyspark.sql import DataFrame
import os

def df_transform_date(df: DataFrame)->DataFrame:
       return df.groupBy("event_date").agg(sum("event_count").alias("total_events")).sort(desc("total_events"))

def write_df(df: DataFrame)->DataFrameWriter:
    df.repartition(1).write.format("csv").mode("overwrite").save("02_data_aggregation\output.csv")

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
