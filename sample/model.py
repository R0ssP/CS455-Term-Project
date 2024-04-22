import sys
from datetime import datetime

import pyspark # type: ignore

from pyspark.sql import SparkSession, Row

from pyspark.sql.functions import col,udf


from pyspark.sql.types import IntegerType

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

from functools import reduce
from Util import scrub_colum_array, calculate_response_time
import geopandas as gpd
from Util import  scrub_colum_array,  calculate_response_time

import pandas
import numpy as np
from shapely.geometry import Polygon, Point

spark = SparkSession.builder \
.master("local")\
.appName('crime_solver')\
.config("spark.executor.memory", "4g")\
.getOrCreate()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")




crime_df = spark.read.csv("NYPD.csv", header=True)

weather_df = spark.read.csv("NYCW.csv", header=True)
weather_df = weather_df.withColumnRenamed("Day", "DATE")
weather_df.show(5)




# for new orleans the input is:
# 1 zipcode 18 10 11
# for NYPD the input is:
# 10 longitude / latitude 16 17 13 14

#ny dataset:
# type of crime is 10, lat is 16, long is 17, dispatch is 13, arrival is 14, center latitude is 40.958, long is -73.9588


paramIndexArray = [10,16,17,13,14]

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
    calculate_response_time_udf(col(filtered_crime_columns[2]), col(filtered_crime_columns[3]))  # Pass entire row to UDF
)

# Show DataFrame with response_time column
filtered_crime_df.show(5)

filtered_crime_df = filtered_crime_df.drop(filtered_crime_columns[2])
filtered_crime_df = filtered_crime_df.drop(filtered_crime_columns[3])
filtered_crime_df.show(5)


# get the uniwue column first column values, write a udf to map the values to index for new column
unique_events = filtered_crime_df.select(filtered_crime_columns[1]).distinct().rdd.map(lambda row: row[0]).collect()

def map_event_type(eventTypes):
    def map_event_type_udf(event_value):
        for i, e in enumerate(eventTypes):
            if event_value == e:
                return i
        return -1

    return udf(map_event_type_udf, IntegerType())

filtered_crime_df = filtered_crime_df.withColumn(
    "event_type_value",
    map_event_type(unique_events)(col(filtered_crime_columns[1]))
)

filtered_crime_df.show(5)



# # Select DATE, PRCP, TMIN, and TMAX columns from weather DataFrame
# weather_df = weather_df.select("DATE", "PRCP", "TMIN", "TMAX")

# # Calculate the average of TMIN and TMAX and add as a new column 'TAVG'
# weather_df = weather_df.withColumn("TAVG", (col("TMIN") + col("TMAX")) / 2)

# # Join DataFrames on DATE column
# final_weather_df = weather_df.select("DATE", "PRCP", "TAVG")

# # Write joined DataFrame to CSV

# final_weather_df.write.csv("NY_weather_processed.csv", header=True, mode="overwrite")
# final_weather_df = final_weather_df.withColumn("DATE", col("DATE").cast("date"))
# final_weather_df = final_weather_df.withColumn("DATE", date_format(col("DATE"), "MM/dd/yyyy"))
# final_weather_df = final_weather_df.filter(col("DATE").substr(7,3) == "202")
# final_weather_df.show(10)

# function(s) for creating logical grid
# side length for nyc is 48km16
# side length for NO is 22km
#corners = get_grid_edges()

def create_grid(xmin, xmax, ymin, ymax, width, height):
    rows = int(np.ceil((ymax-ymin) / height))
    cols = int(np.ceil((xmax-xmin) / width))
    x_left = xmin
    x_right = xmin + width
    grid_cells = []
    for i in range(cols):
        y_top = ymax
        y_bottom = ymax - height
        for j in range(rows):
            grid_cells.append(Polygon([(x_left, y_top), (x_right, y_top), (x_right, y_bottom), (x_left, y_bottom)]))
            y_top = y_bottom
            y_bottom = y_bottom - height
        x_left = x_right
        x_right = x_right + width
    grid = gpd.GeoDataFrame(grid_cells, columns=['geometry'])
    grid.crs = {'init': 'epsg:4326'}
    return grid



crime_df = filtered_crime_df.toPandas()
crime_df['geometry'] = crime_df.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)
crime_gdf = gpd.GeoDataFrame(crime_df, geometry='geometry', crs={'init': 'epsg:4326'})


xmin, xmax, ymin, ymax = -74.25559, -73.70001, 40.49612, 40.91553
width = 0.01  # width of a grid cell in longitude degrees, adjust as necessary
height = 0.01  # height of a grid cell in latitude degrees, adjust as necessary
grid = create_grid(xmin, xmax, ymin, ymax, width, height)
crime_with_grid = gpd.sjoin(crime_gdf, grid, how="inner", op='within')
crime_with_grid = crime_with_grid.drop("geometry", axis=1)

print(crime_with_grid.head(10))
filtered_crime_df = spark.createDataFrame(crime_with_grid)
filtered_crime_df.show(10)

filtered_crime_df = filtered_crime_df.withColumnRenamed('index_right', 'zone')
filtered_crime_df = filtered_crime_df.withColumnRenamed('INCIDENT_DATE', 'DATE')
filtered_crime_df.show(10)
weather_df.show(10)


filtered_crime_df = filtered_crime_df.join(weather_df, on='DATE', how='left')



filtered_crime_df = filtered_crime_df.filter(col("DATE").substr(7,4) != "2022")
filtered_crime_df = filtered_crime_df.withColumn("High (°F)", col("High (°F)").cast("float"))
filtered_crime_df = filtered_crime_df.withColumn("Low (°F)", col("Low (°F)").cast("float"))
filtered_crime_df = filtered_crime_df.withColumn("`Precip. (inches)`", col("`Precip. (inches)`").cast("float"))
filtered_crime_df = filtered_crime_df.withColumn("Snow (inches)", col("Snow (inches)").cast("float"))

# Check the schema to confirm the data type changes
print("about to save frame")
frame_path = "/user/jdy2003/nycFrame/"
filtered_crime_df.write.format("csv").save(frame_path)
print("write complete")



# below is basically what should happen, I think it will run but don't have time to chcek rn

# feature_cols = ['event_type_value', 'zone', 'High (°F)', 'Low (°F)', 'Precip. (inches)', 'Snow (inches)']
# assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
# lr = LinearRegression(featuresCol="features", labelCol="response_time_in_minutes")
# pipeline = Pipeline(stages=[assembler, lr])

# train_data, test_data = filtered_crime_df.randomSplit([0.8,0.2], seed=45)

# nyc_crime_model = pipeline.fit(train_data)
# predictions = nyc_crime_model.transform(test_data)

# model = pipeline.fit(train_data)
# predictions = model.transform(test_data)

# evaluator = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="rmse")
# rmse = evaluator.evaluate(predictions)
# print("Root Mean Squared Error (RMSE):", rmse)


# model_path = "/user/jdy2003/NYCModel"
# nyc_crime_model.save(model_path)


spark.stop()

#  create the vectored columns for training
# get the subset
# train that shit
# write the analysis files
