import os
os.environ["PYSPARK_PYTHON"] = "/s/bach/j/under/jdy2003/miniconda3/bin/python3.12"
from datetime import datetime

import pyspark 

from pyspark.sql import SparkSession

from pyspark.sql.functions import col,udf


from pyspark.sql.types import IntegerType, StructType, FloatType, StringType, StructField

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Polygon, Point
#from geospark.sql.functions import ST_Within


spark = SparkSession.builder \
.appName('crime_solver')\
.getOrCreate()

filtered_crime_df = spark.read.csv("partition_3", header=True)

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
print(grid.head(20))
crime_with_grid = gpd.sjoin(crime_gdf, grid, how="inner", op='within')
print(crime_with_grid['geometry'])

def extract_center_coords(point):
    return point.y, point.x


crime_with_grid[['zone_lat','zone_lon']] = crime_with_grid['geometry'].apply(lambda point: pd.Series(extract_center_coords(point)))
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
#print(filtered_crime_df.count())
spark.stop()
