import sys
from datetime import datetime
spark_python_path = '/usr/local/spark/3.5.0-with-hadoop3.3/python'
sys.path.append(spark_python_path)
import pyspark
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import IntegerType

from functools import reduce
from Util import get_params, scrub_colum_array, get_file
from gridgenerator import get_grid_edges;


spark = SparkSession.builder \
.master("local")\
.appName('crime_solver')\
.getOrCreate()

named_frame = spark.read.csv(get_file(), header=True) 





# for new orleans the input is:
# 1 zipcode 18 10 11
# for NYPD the input is:
# 10 longitude / latitude 16 17 13 14

paramIndexArray = get_params()

column_list = named_frame.columns
column_list = scrub_colum_array(column_list, paramIndexArray)

for item in column_list:
    named_frame = named_frame.drop(item)

#named_frame.show(10)
#print(named_frame.count())

column_list = named_frame.columns
print(list(column_list))

conditions = [col(column).isNotNull() for column in named_frame.columns]

filtered_frame = named_frame.filter(reduce(lambda a, b: a & b, conditions))

#filtered_frame.show(10)
#print(filtered_frame.count())



# create the response time column

filtered_columns = list(filtered_frame.columns)
print(filtered_columns)


def calculate_response_time(time_left, time_arrived):
    timestamp1 = datetime.strptime(time_left, "%m/%d/%Y %I:%M:%S %p")
    timestamp2 = datetime.strptime(time_arrived, "%m/%d/%Y %I:%M:%S %p")
    response_time_string = timestamp2 - timestamp1
    response_time_minutes = int(response_time_string.total_seconds() / 60)
    return response_time_minutes

# Add an empty column 'response_time'


# Register UDF
calculate_response_time_udf = udf(calculate_response_time, IntegerType())

# Apply UDF to each row
filtered_frame = filtered_frame.withColumn(
    "response_time_in_minutes",
    calculate_response_time_udf(col(filtered_columns[1]), col(filtered_columns[2]))  # Pass entire row to UDF
)

# Show DataFrame with response_time column
filtered_frame.show(5)

filtered_frame = filtered_frame.drop(filtered_columns[1])
filtered_frame = filtered_frame.drop(filtered_columns[2])
filtered_frame.show(5)


# function(s) for creating logical grid
# side length for nyc is 55km,
# side length for NO is 22km
#corners = get_grid_edges()
