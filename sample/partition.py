from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("partition data") \
    .getOrCreate()

# Load your DataFrame
massive_frame = spark.read.csv("/user/jdy2003/filteredFrame/", header=True)

# Add a new column with a unique identifier to hash on
massive_frame = massive_frame.withColumn("hash", F.rand(seed=42))

# Perform hash partitioning based on the new column
#massive_frame = massive_frame.repartition(4, "hash")

# Drop the hash column
# partitioned_frames = partitioned_frames.drop("hash")
massive_frame.show(10)

massive_frame = massive_frame.withColumn("hash", F.col("hash") * 10)
massive_frame.show(10)

def map_partitions(hash_value):
    return hash_value

map_partitions_udf = F.udf(map_partitions, IntegerType())

massive_frame = massive_frame.withColumn("partition_id", F.col("hash").substr(1,1) % 3)
massive_frame = massive_frame.drop(F.col("hash"))
massive_frame.show(50)



# Save the partitioned DataFrames
for i in range(3):
    massive_frame.filter(F.col("partition_id") == float(i)) \
        .drop("partition_id") \
        .write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("/user/jdy2003/partition_" + str(i))




# crime_df = spark.read.csv("NYPD.csv", header=True)

# # weather_df = spark.read.csv("NY_weather.csv", header=True)
# # weather_df.show(5)


# # for new orleans the input is:
# # 1 zipcode 18 10 11
# # for NYPD the input is:
# # 10 longitude / latitude 16 17 13 14

# #ny dataset:
# # type of crime is 10, lat is 16, long is 17, dispatch is 13, arrival is 14, center latitude is 40.958, long is -73.9588


# paramIndexArray =  [11,17,18,14,15]  #[10,16,17,13,14]

# column_list = crime_df.columns
# print(column_list)
# column_list = scrub_colum_array(column_list, paramIndexArray)

# for item in column_list:
#     crime_df = crime_df.drop(item)

# #named_frame.show(10)
# #print(named_frame.count())

# column_list = crime_df.columns
# print(list(column_list))

# conditions = [col(column).isNotNull() for column in crime_df.columns]

# filtered_crime_df = crime_df.na.drop()  #filter(reduce(lambda a, b: a & b, conditions))

# #filtered_crime_df.show(10)
# #print(filtered_crime_df.count())

# # create the response time column

# filtered_crime_columns = list(filtered_crime_df.columns)
# print(filtered_crime_columns)
# filtered_crime_df.show(25)


# def calculate_response_time(time_left, time_arrived):
#     timestamp1 = datetime.strptime(time_left, "%m/%d/%Y %I:%M:%S %p")
#     timestamp2 = datetime.strptime(time_arrived, "%m/%d/%Y %I:%M:%S %p")
#     response_time_string = timestamp2 - timestamp1
#     response_time_minutes = int(response_time_string.total_seconds() / 60)
#     return response_time_minutes

# # Add an empty column 'response_time'


# # Register UDF
# calculate_response_time_udf = udf(calculate_response_time, IntegerType())

# # Apply UDF to each row
# filtered_crime_df = filtered_crime_df.withColumn(
#     "response_time_in_minutes",
#     calculate_response_time_udf(col(filtered_crime_columns[2]), col(filtered_crime_columns[3]))  # Pass entire row to UDF
# )

# filtered_crime_df = filtered_crime_df.filter(col("response_time_in_minutes") > 0)
# filtered_crime_df = filtered_crime_df.na.drop()

# # Show DataFrame with response_time column

# filtered_crime_df = filtered_crime_df.drop(filtered_crime_columns[2])
# filtered_crime_df = filtered_crime_df.drop(filtered_crime_columns[3])

# # # get the uniwue column first column values, write a udf to map the values to index for new column
# # unique_events = filtered_crime_df.select(filtered_crime_columns[1]).distinct().rdd.map(lambda row: row[0]).collect()

# # def map_event_type(eventTypes):
# #     def map_event_type_udf(event_value):
# #         for i, e in enumerate(eventTypes):
# #             if event_value == e:
# #                 return i
# #         return -1

# #     return udf(map_event_type_udf, IntegerType())

# # filtered_crime_df = filtered_crime_df.withColumn(
# #     "event_type_value",
# #     map_event_type(unique_events)(col(filtered_crime_columns[1]))
# # )

# filtered_crime_df.show(10)