from datetime import datetime
import os
os.environ["PYSPARK_PYTHON"] = "/s/bach/j/under/jdy2003/miniconda3/bin/python3.12"
import pyspark 

from pyspark.sql import SparkSession

from pyspark.sql.functions import col,asc, count,desc


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

spark = SparkSession.builder.appName("clean crashes").getOrCreate()

crash_data = spark.read.csv("/user/jdy2003/crashes.csv", header=True)

crash_data = crash_data.select("CRASH DATE", "LATITUDE", "LONGITUDE")

crash_data.show(10)

crash_data = crash_data.na.drop()
crash_data = crash_data.withColumnRenamed("CRASH DATE", "DATE")

crash_data = crash_data.filter(col("DATE").substr(7,4) > "2017")

crash_data = crash_data.orderBy(asc("DATE"))

crash_data = crash_data.filter(col("LATITUDE") != 0)
crash_data.show(10)

crash_data = crash_data.select(
    col("DATE"),
    col("LATITUDE").cast("float"),
    col("LONGITUDE").cast("float")
)

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

crash_pdf = crash_data.toPandas()
crash_pdf['geometry'] = crash_pdf.apply(lambda row: Point(row['LONGITUDE'], row['LATITUDE']), axis=1)
crash_gpdf = gpd.GeoDataFrame(crash_pdf, geometry='geometry', crs={'init':'epsg:4326'})
xmin, xmax, ymin, ymax = -74.25559, -73.70001, 40.49612, 40.91553
width = 0.01  # width of a grid cell in longitude degrees, adjust as necessary about 1.1km
height = 0.01  # height of a grid cell in latitude degrees, adjust as necessary
grid = create_grid(xmin, xmax, ymin, ymax, width, height)

crash_with_grid = gpd.sjoin(crash_gpdf, grid, how='inner', op='within')
crash_with_grid = crash_with_grid.drop("geometry", axis=1)
crash_with_zones = spark.createDataFrame(crash_with_grid)
crash_with_zones = crash_with_zones.withColumnRenamed("index_right", "zone")
crash_with_zones.show(10)

grouped_crashes = crash_with_zones.groupBy("DATE","zone").agg(count("*").alias("accident_count"))
grouped_crashes = grouped_crashes.orderBy(desc("accident_count"))
grouped_crashes.show(10)

grouped_crashes.write.format("csv").option("header","true").mode("overwrite").save("/user/jdy2003/crashes")

spark.stop()