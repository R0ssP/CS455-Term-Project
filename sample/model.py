from datetime import datetime

import pyspark 

from pyspark.sql import SparkSession

from pyspark.sql.functions import col,udf


from pyspark.sql.types import IntegerType

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

from functools import reduce

import geopandas as gpd
import pandas
import numpy as np
from shapely.geometry import Polygon, Point
#from geospark.sql.functions import ST_Within


spark = SparkSession.builder \
.master("local")\
.appName('crime_solver')\
.config("spark.executor.memory", "8g")\
.getOrCreate()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")



filtered_crime_df = spark.read.csv("partition_3", header=True)




# weather_df = spark.read.csv("NY_weather.csv", header=True)
# weather_df.show(5)


# # for new orleans the input is:
# # 1 zipcode 18 10 11
# # for NYPD the input is:
# # 10 longitude / latitude 16 17 13 14


# #filtered_crime_df.show(10)
# #print(filtered_crime_df.count())

# # create the response time column

filtered_crime_columns = list(filtered_crime_df.columns)
print(filtered_crime_columns)
# filtered_crime_df.show(25)


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

filtered_crime_df.show(10)

# # Select DATE, PRCP, TMIN, and TMAX columns from weather DataFrame
# weather_df = weather_df.select("DATE", "PRCP", "TMIN", "TMAX")

# # Calculate the average of TMIN and TMAX and add as a new column 'TAVG'
# weather_df = weather_df.withColumn("TAVG", (col("TMIN") + col("TMAX")) / 2)

# # Join DataFrames on DATE column
# final_weather_df = weather_df.select("DATE", "PRCP", "TAVG")


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


filtered_crime_df = filtered_crime_df.withColumn("Longitude", col("Longitude").cast("float"))
filtered_crime_df = filtered_crime_df.withColumn("Latitude", col("Latitude").cast("float"))
crime_df = filtered_crime_df.toPandas()

crime_df['geometry'] = crime_df.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)
crime_gdf = gpd.GeoDataFrame(crime_df, geometry='geometry', crs={'init': 'epsg:4326'})


xmin, xmax, ymin, ymax = -74.25559, -73.70001, 40.49612, 40.91553
width = 0.01  # width of a grid cell in longitude degrees, adjust as necessary about 1.1km
height = 0.01  # height of a grid cell in latitude degrees, adjust as necessary
grid = create_grid(xmin, xmax, ymin, ymax, width, height)
crime_with_grid = gpd.sjoin(crime_gdf, grid, how="inner", op='within')
crime_with_grid = crime_with_grid.drop("geometry", axis=1)

filtered_crime_df = spark.createDataFrame(crime_with_grid)
filtered_crime_df.show(10)

filtered_crime_df = filtered_crime_df.withColumnRenamed('index_right', 'zone')
filtered_crime_df = filtered_crime_df.withColumnRenamed('INCIDENT_DATE', 'DATE')
filtered_crime_df.show(10)


# filtered_crime_df = filtered_crime_df.join(weather_df, on='DATE', how='left')

def date_to_day(date):
    date = datetime.strptime(date, '%m/%d/%Y')
    return date.timetuple().tm_yday

date_to_day_udf = udf(date_to_day, IntegerType())

filtered_crime_df = filtered_crime_df.na.drop()
filtered_crime_df = filtered_crime_df.withColumn("DayOfYear", date_to_day_udf(filtered_crime_df["DATE"]))
print("about to save frame")
frame_path = "/user/jdy2003/iPartition_3/"
filtered_crime_df.write.mode("overwrite").option("header", "true").csv(frame_path)
print("write complete")
filtered_crime_df.show(20)
# #print(filtered_crime_df.count())
spark.stop()
