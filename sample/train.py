from datetime import datetime

import pyspark # type: ignore

from pyspark.sql import SparkSession, Row

from pyspark.sql.functions import col, regexp_extract


from pyspark.sql.types import IntegerType

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon, Point

spark = SparkSession.builder.appName("train model").getOrCreate()

crime_data = spark.read.csv("/user/jdy2003/nycFrame/", header=True)
crime_data.show(10)

crime_data = crime_data.filter(col("response_time_in_minutes") < 10)
crime_count = crime_data.count()


crime_data = crime_data.withColumn("TAVG", col("TAVG").cast("float"))
crime_data = crime_data.withColumn("PRCP", col("PRCP").cast("float"))
crime_data = crime_data.withColumn("event_type_value", col("event_type_value").cast("float"))
crime_data = crime_data.withColumn("zone", col("zone").cast("float"))
crime_data = crime_data.withColumn("DayOfYear", col("DayOfYear").cast("float"))
crime_data = crime_data.withColumn("response_time_in_minutes", col("response_time_in_minutes").cast("float"))
crime_data = crime_data.withColumn("Latitude", col("Latitude").cast("float"))
crime_data = crime_data.withColumn("Longitude", col("Longitude").cast("float"))

# # Check for non-numeric entries in the column
crime_data.na.drop()

feature_cols = ['event_type_value', 'zone','DayOfYear']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
train_data, test_data = crime_data.randomSplit([0.8,0.2], seed=42)
evaluator = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="rmse")

# XGBoost
xgb = GBTRegressor(featuresCol="features", labelCol="response_time_in_minutes")

pipeline_xgb = Pipeline(stages=[assembler, xgb])

model_xgb = pipeline_xgb.fit(train_data)
predictions_xgb = model_xgb.transform(test_data)

evaluator_rmse = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="rmse")
rmse_xgb = evaluator_rmse.evaluate(predictions_xgb)

evaluator_r2 = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction",metricName="r2")
r2_xgb = evaluator_r2.evaluate(predictions_xgb)

evaluator_ma = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction",metricName="mae")
ma_xgb = evaluator_ma.evaluate(predictions_xgb)


# Save the model
model_path = "/user/jdy2003/xgbModel"
model_xgb.write().overwrite().save(model_path)



# BELOW IS THE CODE FOR DECISION TREE REGRESSION
# dt = DecisionTreeRegressor(featuresCol="features", labelCol="response_time_in_minutes")
# pipeline_dt = Pipeline(stages=[assembler, dt])

# model_dt = pipeline_dt.fit(train_data)
# predictions_dt = model_dt.transform(test_data)

# rmse_dt = evaluator.evaluate(predictions_dt)
# print("Decision Tree RMSE:", rmse_dt)

# # Random Forests
# rf = RandomForestRegressor(featuresCol="features", labelCol="response_time_in_minutes")
# pipeline_rf = Pipeline(stages=[assembler, rf])

# model_rf = pipeline_rf.fit(train_data)
# predictions_rf = model_rf.transform(test_data)

# rmse_rf = evaluator.evaluate(predictions_rf)
# print("Random Forest RMSE:", rmse_rf)

# BELOW IS THE CODE FOR REGULAR LR
# pipeline = Pipeline(stages=[assembler, lr])

# train_data, test_data = crime_data.randomSplit([0.8,0.2], seed=45)

# nyc_crime_model = pipeline.fit(train_data)
# predictions = nyc_crime_model.transform(test_data)

# model = pipeline.fit(train_data)
# predictions = model.transform(test_data)

# evaluator = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="rmse")
# rmse = evaluator.evaluate(predictions)
# print("Root Mean Squared Error (RMSE):", rmse)


# model_path = "/user/jdy2003/rfModel"
# model_rf.write().overwrite().save(model_path)

print("no way!")
print("XGBoost RMSE:", rmse_xgb)
print("XGBoost R Squared: ", r2_xgb)
print("XGBoost MAE: ", ma_xgb)
print(crime_count)

spark.stop()