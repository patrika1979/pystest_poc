from data_transformation import *

#def test_data_transformation(spark_session):
#   assert spark_session is not None
from pyspark import *
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, DoubleType
import pandas as pd

import os
import sys


def test_data_transformation(spark_session):
    
  working_path = os.getcwd()
  filename = f"{working_path}\\01_data_transformation\\data.csv"
  
  test_data = [
      { "user_id": 1,
        "avg_duration": 6.5
        
      },
        { "user_id": 2,
        "avg_duration": 10.0
      }
      
  ]
  
  schema = StructType([ \
    StructField("user_id",IntegerType(),True), \
    StructField("timestamp",TimestampType(),True), \
    StructField("event_type",StringType(),True), \
    StructField("duration", DoubleType(), True)
  ])
  
  schema_teste = StructType([ \
    StructField("user_id",IntegerType(),True), \
    StructField("avg_duration",DoubleType(),True)
  ])
      
  df_teste = spark_session.createDataFrame(map(lambda x: Row(**x), test_data), schema=schema_teste).collect()
  
  df_output = data_transformation(spark_session, filename, "event_type", schema).collect()
   
  assert df_output==df_teste
  
