from pyspark.sql import *
from pyspark.sql.functions import col, sum
from pyspark.sql import DataFrame
import os

def df_transform_calc(df: DataFrame)->DataFrame:
       return df.groupBy("user_id").agg(sum("price").alias("tot_spending"))


def write_df(df: DataFrame)->DataFrameWriter:
    df.repartition(1).write.format("parquet").mode("overwrite").save("03_data_join\output.json")

def df_rename(df: DataFrame, column)->DataFrame:
    return df.withColumn(f"{column}_1", col("user_id")).drop("user_id")
        
def join_datasets(df_01:DataFrame, df_02: DataFrame)->DataFrame:
    return df_01.join(df_02, df_02["user_id"]==df_01["user_id_1"], "inner")

def read_data(spark, filename: str)-> DataFrame:
    df = spark.read.format("csv")\
                          .option("header", True)\
                          .option("delimiter", ",")\
                          .option("inferSchema", "true") \
                          .load(filename)
    return df

def data_join(spark, filename1, filename2):
    df_users = read_data(spark, filename1)
    df_purchases = read_data(spark, filename2)
    
    df_users_ren = df_rename(df_users, "user_id")
    
    df_join = join_datasets(df_users_ren, df_purchases)
    
    df_final = df_transform_calc(df_join)
    
    write_df(df_final)
    
    return df_final
