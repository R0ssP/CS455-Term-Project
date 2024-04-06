import sys
spark_python_path = '/usr/local/spark/3.5.0-with-hadoop3.3/python'
sys.path.append(spark_python_path)
import pyspark
from pyspark.sql import Row
from pyspark.sql import SparkSession
from Scrubber import get_params, scrub_colum_array


spark = SparkSession.builder \
.master("local")\
.appName('word_count')\
.getOrCreate()

named_frame = spark.read.csv("neworleans.csv", header=True) 

named_frame.show(10)

# for new orleans the input is:
# 1 zipcode 18 10 11
paramIndexArray = get_params()

column_list = named_frame.columns
column_list = scrub_colum_array(column_list, paramIndexArray)

for item in column_list:
    named_frame = named_frame.drop(item)

named_frame.show(10)
