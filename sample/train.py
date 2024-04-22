from datetime import datetime

import pyspark # type: ignore

from pyspark.sql import SparkSession, Row

from pyspark.sql.functions import col, regexp_extract


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

spark = SparkSession.builder.appName("train model").getOrCreate()

crime_data = spark.read.csv("/user/jdy2003/nycFrame/", header=True)
crime_data.show(10)
print(crime_data.count())


crime_data = crime_data.filter(col("DATE").substr(7,4) != "2022")
crime_data = crime_data.withColumn("high_in_F", col("high_in_F").cast("float"))
crime_data = crime_data.withColumn("low_in_F", col("low_in_F").cast("float"))
crime_data = crime_data.withColumn("precip_in_inches", col("precip_in_inches").cast("float"))
crime_data = crime_data.withColumn("snow_in_inches", col("snow_in_inches").cast("float"))
crime_data = crime_data.withColumn("event_type_value", col("event_type_value").cast("float"))
crime_data = crime_data.withColumn("zone", col("zone").cast("float"))
crime_data = crime_data.withColumn("DayOfYear", col("DayOfYear").cast("float"))
crime_data = crime_data.withColumn("response_time_in_minutes", col("response_time_in_minutes").cast("float"))

# Check for non-numeric entries in the column
crime_data.na.drop()

feature_cols = ['event_type_value', 'zone', 'high_in_F', 'low_in_F', 'precip_in_inches', 'snow_in_inches','DayOfYear']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
lr = LinearRegression(featuresCol="features", labelCol="response_time_in_minutes")
pipeline = Pipeline(stages=[assembler, lr])

train_data, test_data = crime_data.randomSplit([0.8,0.2], seed=45)

nyc_crime_model = pipeline.fit(train_data)
predictions = nyc_crime_model.transform(test_data)

model = pipeline.fit(train_data)
predictions = model.transform(test_data)

evaluator = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE):", rmse)


model_path = "/user/jdy2003/NYCModel"
nyc_crime_model.save(model_path)

print("no way!")

spark.stop()