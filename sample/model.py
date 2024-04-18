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

crime_df = spark.read.csv(get_file(), header=True)




# for new orleans the input is:
# 1 zipcode 18 10 11
# for NYPD the input is:
# 10 longitude / latitude 16 17 13 14

#ny dataset:
# type of crime is 10, lat is 16, long is 17, dispatch is 13, arrival is 14, center latitude is 40.958, long is -73.9588


paramIndexArray = get_params()

column_list = crime_df.columns
print(column_list)
column_list = scrub_colum_array(column_list, paramIndexArray)

for item in column_list:
    crime_df = crime_df.drop(item)

#named_frame.show(10)
#print(named_frame.count())

column_list = crime_df.columns
print(list(column_list))

conditions = [col(column).isNotNull() for column in crime_df.columns]

filtered_crime_df = crime_df.filter(reduce(lambda a, b: a & b, conditions))

#filtered_crime_df.show(10)
#print(filtered_crime_df.count())



# create the response time column

filtered_crime_columns = list(filtered_crime_df.columns)
print(filtered_crime_columns)


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
filtered_crime_df = filtered_crime_df.withColumn(
    "response_time_in_minutes",
    calculate_response_time_udf(col(filtered_crime_columns[1]), col(filtered_crime_columns[2]))  # Pass entire row to UDF
)

# Show DataFrame with response_time column
filtered_crime_df.show(5)

filtered_crime_df = filtered_crime_df.drop(filtered_crime_columns[1])
filtered_crime_df = filtered_crime_df.drop(filtered_crime_columns[2])
filtered_crime_df.show(5)

# get the uniwue column first column values, write a udf to map the values to index for new column
unique_events = filtered_crime_df.select(filtered_crime_columns[0]).distinct().rdd.map(lambda row: row[0]).collect()

def map_event_type(eventTypes):
    def map_event_type_udf(event_value):
        for i, e in enumerate(eventTypes):
            if event_value == e:
                return i
        return -1

    return udf(map_event_type_udf, IntegerType())

filtered_crime_df = filtered_crime_df.withColumn(
    "event_type_value",
    map_event_type(unique_events)(col(filtered_crime_columns[0]))
)

filtered_crime_df.show(5)

# Read weather data CSV into a DataFrame
weather_df = spark.read.csv("NY_weather.csv", header=True)

# Select DATE, PRCP, TMIN, and TMAX columns from weather DataFrame
weather_df = weather_df.select("DATE", "PRCP", "TMIN", "TMAX")

# Calculate the average of TMIN and TMAX and add as a new column 'TAVG'
weather_df = weather_df.withColumn("TAVG", (col("TMIN") + col("TMAX")) / 2)

# Join DataFrames on DATE column
final_weather_df = weather_df.select("DATE", "PRCP", "TAVG")

# Write joined DataFrame to CSV

final_weather_df.write.csv("NY_weather_processed.csv", header=True, mode="overwrite")
final_weather_df.show(5)

spark.stop()


# function(s) for creating logical grid
# side length for nyc is 48km16
# side length for NO is 22km
#corners = get_grid_edges()

grid = generate_grid()
print(grid[0])

<<<<<<< Updated upstream
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def map_to_zone(lat, lon, grid):
    for i, row in enumerate(grid):
        if (row[1][0] <= lat <= row[0][0]) and (row[1][1] <= lon <= row[0][1]):
            return i
=======
# def map_to_zone(lat, lon, grid):
#     for i, row in enumerate(grid):
#         if (row[1][0] <= lat <= row[0][0]) and (row[1][1] <= lon <= row[0][1]):
#             return i
>>>>>>> Stashed changes
    
#     return -1

<<<<<<< Updated upstream
map_to_zone_udf = udf(map_to_zone, IntegerType())
# map_to_zone_udf = udf(lambda lat, lon, grid: map_to_zone(lat, lon, grid), IntegerType())
filtered_crime_columns = list(filtered_crime_df.columns)
=======
# lat = 40.2430
# lon = -73.0059008
# zone = map_to_zone(lat, lon, grid)
# print("Zone:", zone)

# map_to_zone_udf = udf(lambda lat, lon, grid: map_to_zone(lat, lon, grid), IntegerType())
# filtered_crime_columns = list(filtered_crime_df.columns)


# # Apply the UDF to the DataFrame to create a new column
# filtered_crime_df = filtered_crime_df.withColumn(
#     "zone",
#     map_to_zone_udf(col(filtered_crime_columns[3]), col(filtered_crime_columns[4]), lit(grid))
# )

# filtered_crime_df.show(5)


def map_me(grid):
    def map_me_udf(lat, lon):
        for i, row in enumerate(grid):
            if (row[1][0] <= lat <= row[0][0]) and (row[1][1] <= lon <= row[0][1]):
                return i
    
        return -1
    return udf(map_me_udf, IntegerType())

>>>>>>> Stashed changes


filtered_crime_df = filtered_crime_df.withColumn(
    "zone",
<<<<<<< Updated upstream
    map_to_zone_udf(col("Latitude"), col("Longitude"), lit(grid))
=======
    map_me(grid)(col(filtered_crime_columns[3]), col(filtered_crime_columns[4]))
>>>>>>> Stashed changes
)

filtered_crime_df.show(5)





# # Define a function to map latitude and longitude to a zone
# def map_to_zone(lat, lon):
#     for i, row in enumerate(grid):
#         for j, points in enumerate(row):
#             # Check if the latitude and longitude fall within the current grid cell
#             if (points[2][0] <= lat <= points[0][0]) and (points[0][1] <= lon <= points[2][1]):
#                 return i * len(grid) + j  # Assuming a unique integer value for each zone
#     return -1  # Return -1 if the point does not fall within any grid cell

# # Register the UDF (User Defined Function)
# from pyspark.sql.functions import udf
# from pyspark.sql.types import IntegerType

# map_to_zone_udf = udf(map_to_zone, IntegerType())

# # Apply the UDF to each row of the DataFrame to create the "zone" column
# df = df.withColumn("zone", map_to_zone_udf("latitude_column", "longitude_column"))

# # Show the DataFrame with the new "zone" column
# df.show()



# grid_rows = [Row(lat_upper_bound=grid[i][0][0], long_upper_bound=grid[i][0][1], 
#                   lat_lower_bound=grid[i][2][0], long_lower_bound=grid[i][2][1]) for i in range(len(grid))]

# grid_df = spark.createDataFrame(grid_rows)

# grid_df = grid_df.withColumn("index", monotonically_increasing_id())
# grid_df.printSchema()
# grid_df.show(1)


# grid_df.printSchema()
# grid_df.show(1)


# lat_upper_bound_list = [float(row.lat_upper_bound) for row in grid_df.collect()]
# long_upper_bound_list = [float(row.long_upper_bound) for row in grid_df.collect()]
# lat_lower_bound_list = [float(row.lat_lower_bound) for row in grid_df.collect()]
# long_lower_bound_list = [float(row.long_lower_bound) for row in grid_df.collect()]

# @udf(IntegerType())
# def map_to_zone(lat, long, lat_upper_bound, long_upper_bound, lat_lower_bound, long_lower_bound):
#     # for i in range(len(lat_upper_bound)):
#     #     #print(float(long) - float(lat)) # sanity check, it failed, always the same values
#     #     print(lat_lower_bound[0])
#     #     print(lat_lower_bound[10])
#     #     if (long_lower_bound[i] <= float(long) <= long_upper_bound[i]) and (lat_lower_bound[i] <= float(lat) <= lat_upper_bound[i]):
#     #         return i
#     # return 0


# # Apply the UDF to the DataFrame
# filtered_crime_df = filtered_crime_df.withColumn(
#     "zone",
#     map_to_zone(col(filtered_crime_columns[3]), col(filtered_crime_columns[4]), 
#         lit(lat_upper_bound_list), lit(long_upper_bound_list), 
#         lit(lat_lower_bound_list), lit(long_lower_bound_list))
# )

# filtered_crime_df.show(5)
