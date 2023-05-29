import pytest
from pyspark.sql import SparkSession
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder\
                        .config("spark.sql.sources.partitionOverwriteMode","dynamic") \
                        .config("dfs.client.read.shortcircuit.skip.checksum", "false")\
                        .getOrCreate()
    yield spark
    spark.stop()

