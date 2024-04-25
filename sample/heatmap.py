import os
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point, Polygon
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType
import matplotlib.pyplot as plt
import seaborn as sns

# Setting the PySpark Python environment
os.environ["PYSPARK_PYTHON"] = "/s/bach/j/under/jdy2003/miniconda3/bin/python3.12"

# Initialize Spark session
spark = SparkSession.builder \
    .appName('Grid Income Heatmap') \
    .config("spark.executor.memory", "32g") \
    .getOrCreate()

# Mean income data hardcoded based on boroughs
mean_income = {
    'Bronx': 69157,
    'Brooklyn': 114302,
    'Manhattan': 184058,
    'Queens': 109287,
    'Staten Island': 122431
}

# Create grid based on given geographical boundaries and cell dimensions
def create_grid(xmin, xmax, ymin, ymax, width, height):
    rows = int(np.ceil((ymax - ymin) / height))
    cols = int(np.ceil((xmax - xmin) / width))
    grid_cells = []
    for col_index in range(cols):
        for row_index in range(rows):
            x_left = xmin + col_index * width
            y_top = ymax - row_index * height
            grid_cells.append(Polygon([
                (x_left, y_top),
                (x_left + width, y_top),
                (x_left + width, y_top - height),
                (x_left, y_top - height)
            ]))
    grid = gpd.GeoDataFrame(grid_cells, columns=['geometry'])
    grid.crs = {'init': 'epsg:4326'}
    return grid

# Parameters for grid creation
xmin, xmax, ymin, ymax = -74.25559, -73.70001, 40.49612, 40.91553
width = 0.01  # Approx 1.1 km in longitude degrees
height = 0.01  # Approx 1.1 km in latitude degrees
grid_gdf = create_grid(xmin, xmax, ymin, ymax, width, height)

# Function to determine borough based on coordinates
def get_borough(lat, lon):
    if 40.7850 <= lat <= 40.9153 and -73.9330 <= lon <= -73.7654:
        return 'Bronx'
    elif 40.5704 <= lat <= 40.7395 and -74.0421 <= lon <= -73.8330:
        return 'Brooklyn'
    elif 40.6804 <= lat <= 40.8820 and -74.0479 <= lon <= -73.9070:
        return 'Manhattan'
    elif 40.4895 <= lat <= 40.8012 and -73.9626 <= lon <= -73.7004:
        return 'Queens'
    elif 40.4774 <= lat <= 40.6512 and -74.2590 <= lon <= -74.0346:
        return 'Staten Island'
    else:
        return 'Unknown'

# Convert GeoDataFrame to Spark DataFrame
grid_df = spark.createDataFrame(grid_gdf)

# Define UDF for getting borough
get_borough_udf = udf(get_borough, StringType())

# Add a centroid column for each polygon for determining the borough
grid_df = grid_df.withColumn('latitude', col('geometry').centroid.y)
grid_df = grid_df.withColumn('longitude', col('geometry').centroid.x)

# Assign borough to each grid cell
grid_df = grid_df.withColumn('borough', get_borough_udf(col('latitude'), col('longitude')))

# Map mean income to each grid cell
mean_income_udf = udf(lambda b: mean_income.get(b, 0), IntegerType())
grid_df = grid_df.withColumn('mean_income', mean_income_udf(col('borough')))

# Simulate request time data
grid_df = grid_df.withColumn('request_time', (col('mean_income') / 1000).cast(IntegerType()))

# Convert to Pandas for visualization
grid_pd = grid_df.select('latitude', 'longitude', 'request_time').toPandas()

# Visualization
plt.figure(figsize=(10, 10))
sns.heatmap(grid_pd.pivot("latitude", "longitude", "request_time"), cmap='viridis')
plt.title('Heatmap of Request Times and Mean Income')
plt.show()

# Stop Spark session
spark.stop()
