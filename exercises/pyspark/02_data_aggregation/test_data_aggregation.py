#def test_data_aggregation(spark_session):
#    assert spark_session is not None

from data_aggregation import *

#def test_data_transformation(spark_session):
#   assert spark_session is not None
from pyspark import *
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
import pandas as pd
from datetime import datetime


def test_data_transformation(spark_session):
    
  working_path = os.getcwd()
  filename = f"{working_path}\\02_data_aggregation\\data.json"
  
  test_data = [
      { "event_date": datetime.strptime('2022-01-01', '%Y-%m-%d'),
        "total_events": 18
        
      },
        { "event_date": datetime.strptime("2022-01-02",'%Y-%m-%d'),
        "total_events": 17
      }
      
  ]
  
  schema_teste = StructType([ \
    StructField("event_date",DateType(),True), \
    StructField("total_events",IntegerType(),True)
  ])
  
  schema = StructType([ \
    StructField("user_id",IntegerType(),True), \
    StructField("event_date",DateType(),True), \
    StructField("event_count",IntegerType(),True)
    
  ])
  
  df_teste = spark_session.createDataFrame(map(lambda x: Row(**x), test_data), schema=schema_teste).collect()
                                
  df_output = data_aggregation(spark_session, filename, schema, None).collect()
     
  assert df_output==df_teste
  
