from data_join import *
from pyspark import *
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, DoubleType
import os
import sys

def test_data(spark_session): 
  working_path = os.getcwd()
  file_name1 = f"{working_path}\\03_data_join\\users.csv"
  file_name2 = f"{working_path}\\03_data_join\\purchases.csv"
  test_data = [
      {"user_id": 1,
        "tot_spending": 75.0
        
      },
        {"user_id": 3,
        "tot_spending": 30.0
      },
        {"user_id": 2,
        "tot_spending": 20.0
      }
      
  ]
  
  schema_teste = StructType([ \
    StructField("user_id",IntegerType(),True), \
    StructField("tot_spending",DoubleType(),True)
  ])
      
  df_teste = spark_session.createDataFrame(map(lambda x: Row(**x), test_data), schema=schema_teste).collect()
  
  df_output = data_join(spark_session, file_name1, file_name2).collect()
   
  assert df_output==df_teste