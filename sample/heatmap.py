import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark session
spark = SparkSession.builder \
    .master("local") \
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

# Assuming grid.py has a function called generate_grid that returns a DataFrame of grids
# Each grid cell in DataFrame has 'min_lat', 'max_lat', 'min_lon', 'max_lon'
# This function should ideally exist in grid.py and properly integrated here
from model import create_grid

# Generate grid and simulate data
grid_df = create_grid()

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

get_borough_udf = udf(get_borough, StringType())

# Assign borough to each grid cell
grid_df = grid_df.withColumn('borough', get_borough_udf(col('center_lat'), col('center_lon')))

# Map mean income to each grid cell
mean_income_udf = udf(lambda b: mean_income.get(b, 0), IntegerType())
grid_df = grid_df.withColumn('mean_income', mean_income_udf(col('borough')))

# Simulate request time data
grid_df = grid_df.withColumn('request_time', (col('mean_income') / 1000).cast(IntegerType()))  # Simplified simulation

# Convert to Pandas for visualization
grid_pd = grid_df.toPandas()

# Visualization
plt.figure(figsize=(10, 10))
sns.heatmap(grid_pd.pivot("center_lat", "center_lon", "request_time"), cmap='viridis')
plt.title('Heatmap of Request Times and Mean Income')
plt.show()

# Stop Spark session
spark.stop()
