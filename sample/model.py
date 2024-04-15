import sys
from datetime import datetime
spark_python_path = '/usr/local/spark/3.5.0-with-hadoop3.3/python'
sys.path.append(spark_python_path)
import pyspark
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, udf, broadcast, monotonically_increasing_id
from pyspark.sql.types import IntegerType

from functools import reduce
from Util import get_params, scrub_colum_array, get_file, calculate_response_time
from gridgenerator import generate_grid;


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

print(column_list)
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

# get the uniwue column first column values, write a udf to map the values to index for new column
unique_events = filtered_frame.select(filtered_columns[0]).distinct().rdd.map(lambda row: row[0]).collect()

def map_event_type(eventTypes):
    def map_event_type_udf(event_value):
        for i, e in enumerate(eventTypes):
            if event_value == e:
                return i
        return -1
    
    return udf(map_event_type_udf, IntegerType())

filtered_frame = filtered_frame.withColumn(
    "event_type_value",
    map_event_type(unique_events)(col(filtered_columns[0]))
)

filtered_frame.show(5)



# function(s) for creating logical grid
# side length for nyc is 48km16

# side length for NO is 22km
grid = generate_grid()
print("\n", grid[0])


grid_rows = [Row(lat_upper_bound=grid[i][0][0], long_upper_bound=grid[i][0][1], 
                  lat_lower_bound=grid[i][2][0], long_lower_bound=grid[i][2][1]) for i in range(len(grid))]

grid_df = spark.createDataFrame(grid_rows)

grid_df = grid_df.withColumn("index", monotonically_increasing_id())
grid_df.printSchema()
grid_df.show(1)


lat_upper_bound_list = [float(row.lat_upper_bound) for row in grid_df.collect()]
long_upper_bound_list = [float(row.long_upper_bound) for row in grid_df.collect()]
lat_lower_bound_list = [float(row.lat_lower_bound) for row in grid_df.collect()]
long_lower_bound_list = [float(row.long_lower_bound) for row in grid_df.collect()]

@udf(IntegerType())
def map_to_zone(lat, long, lat_upper_bound, long_upper_bound, lat_lower_bound, long_lower_bound):
    for i in range(len(lat_upper_bound)):
        #print(float(long) - float(lat)) # sanity check, it failed, always the same values
        print(lat_lower_bound[0])
        print(lat_lower_bound[10])
        if (long_lower_bound[i] <= float(long) <= long_upper_bound[i]) and (lat_lower_bound[i] <= float(lat) <= lat_upper_bound[i]):
            return i
    return 0

# Apply the UDF to the DataFrame
filtered_frame = filtered_frame.withColumn(
    "zone",
    map_to_zone(col(filtered_columns[3]), col(filtered_columns[4]), 
                lit(lat_upper_bound_list), lit(long_upper_bound_list), 
                lit(lat_lower_bound_list), lit(long_lower_bound_list))
)

filtered_frame.show(5)

