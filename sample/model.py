import sys
spark_python_path = '/usr/local/spark/3.5.0-with-hadoop3.3/python'
sys.path.append(spark_python_path)
import pyspark
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import reduce
from Scrubber import get_params, scrub_colum_array, get_file
from gridgenerator import get_grid_edges;


spark = SparkSession.builder \
.master("local")\
.appName('crime_solver')\
.getOrCreate()

named_frame = spark.read.csv(get_file(), header=True) 

named_frame.show(10)

# for new orleans the input is:
# 1 zipcode 18 10 11
# for NYPD the input is:
# 10 longitude / latitude 16 17 13 14

paramIndexArray = get_params()

column_list = named_frame.columns
column_list = scrub_colum_array(column_list, paramIndexArray)

for item in column_list:
    named_frame = named_frame.drop(item)

named_frame.show(10)
print(named_frame.count())

column_list = named_frame.columns
print(list(column_list))

conditions = [col(column).isNotNull() for column in named_frame.columns]

filtered_frame = named_frame.filter(reduce(lambda a, b: a & b, conditions))

filtered_frame.show(10)
print(filtered_frame.count())

filtered_columns = list(filtered_frame.columns)

# function(s) for creating logical grid
corners = get_grid_edges()
