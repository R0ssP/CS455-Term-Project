
import pyspark # type: ignore
import os
os.environ["PYSPARK_PYTHON"] = "/s/bach/j/under/jdy2003/miniconda3/bin/python3.12"

from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, desc 

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("train model").getOrCreate()

crime_data = spark.read.csv("/user/jdy2003/nycFrame/", header=True)

crash_data = spark.read.csv("/user/jdy2003/crashes/", header=True)
crash_data = crash_data.orderBy(desc("accident_count"))
crash_data.show(10)
#crime_data.show(10)

crime_data = crime_data.join(crash_data, ["DATE", "zone"], "left")

# If there are null values in accident_count, fill them with 0
# Filter crime data where response_time_in_minutes is less than 10
crime_data = crime_data.filter(col("response_time_in_minutes") < 10)

crime_data = crime_data.fillna("0", subset=["accident_count"])

crime_data.show(10)

crime_data = crime_data.withColumn("TAVG", col("TAVG").cast("float"))
crime_data = crime_data.withColumn("PRCP", col("PRCP").cast("float"))
crime_data = crime_data.withColumn("event_type_value", col("event_type_value").cast("float"))
crime_data = crime_data.withColumn("zone", col("zone").cast("float"))
crime_data = crime_data.withColumn("DayOfYear", col("DayOfYear").cast("float"))
crime_data = crime_data.withColumn("response_time_in_minutes", col("response_time_in_minutes").cast("float"))
crime_data = crime_data.withColumn("Latitude", col("Latitude").cast("float"))
crime_data = crime_data.withColumn("Longitude", col("Longitude").cast("float"))
crime_data = crime_data.withColumn("accident_count", col("accident_count").cast("float"))

# # Check for non-numeric entries in the column
crime_data.na.drop()

feature_cols = ['event_type_value', 'zone','DayOfYear', 'TAVG', 'PRCP', 'accident_count']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
train_data, test_data = crime_data.randomSplit([0.8,0.2], seed=420)
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

print("no way!")
print("XGBoost RMSE:", rmse_xgb)
print("XGBoost R Squared: ", r2_xgb)
print("XGBoost MAE: ", ma_xgb)
predictions_xgb.show(10)
spark.stop()