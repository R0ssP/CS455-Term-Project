from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

import os
os.environ["PYSPARK_PYTHON"] = "/s/bach/j/under/jdy2003/miniconda3/bin/python3.12"

spark = SparkSession.builder.appName("analysis").getOrCreate()


model_schema = StructType([
    StructField("features", StringType(), True),
    StructField("label", FloatType(), True),
    StructField("prediction", FloatType(), True)
])

# Load the model with the specified schema


path_to_model = '/user/jdy2003/best_xgb_model'
predictions = spark.read.csv("/user/jdy2003/predictions", header=True)
crime_data = spark.read.csv("/user/jdy2003/nycFrame", header=True)

cast_columns = ['TAVG', 'PRCP', 'event_type_value', 'zone', 'DayOfYear',
                'response_time_in_minutes', 'Latitude', 'Longitude', 'accident_count']
for column in cast_columns:
    crime_data = crime_data.withColumn(column, F.col(column).cast("float"))

predictions.show()

best_zones = spark.sql("SELECT zone, AVG(response_time_in_miutes) AS avg_response_time FROM predictions GROUP BY zone ORDER BY avg_response_time ASC LIMIT 10")
best_zones.show()

worst_zones = spark.sql("SELECT zone, AVG(response_time_in_miutes) AS avg_response_time FROM predictions GROUP BY zone ORDER BY avg_response_time DESC LIMIT 10")
worst_zones.show()

most_frequent_types = spark.sql("SELECT event_type_value, COUNT(*) AS frequency FROM predictions GROUP BY event_type_value ORDER BY frequency DESC LIMIT 10")
spark.stop()

