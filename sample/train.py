
import pyspark # type: ignore
import os
os.environ["PYSPARK_PYTHON"] = "/s/bach/j/under/jdy2003/miniconda3/bin/python3.12"

from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, desc 

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
import matplotlib.pyplot as plt
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

spark = SparkSession.builder.appName("train model").getOrCreate()

crime_data = spark.read.csv("/user/jdy2003/nycFrame/", header=True)

# column names that we gotta cast
cast_columns = ['TAVG', 'PRCP', 'event_type_value', 'zone', 'DayOfYear',
                'response_time_in_minutes', 'Latitude', 'Longitude', 'accident_count']
for column in cast_columns:
    crime_data = crime_data.withColumn(column, col(column).cast("float"))

# double check no null values exist

crime_data.na.drop()

crime_data.show(10)

feature_cols = ['event_type_value', 'zone','DayOfYear', 'TAVG', 'PRCP', 'accident_count']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

xgb = GBTRegressor(featuresCol="features", labelCol="response_time_in_minutes")

pipeline = Pipeline(stages=[assembler, xgb])

# below builds a parameter grid to search the hyperparam space for optimal combinations
paramGrid = ParamGridBuilder() \
    .addGrid(xgb.maxDepth, [5, 10]) \
    .addGrid(xgb.maxBins, [20, 40]) \
    .build()

evaluator = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="rmse")

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=5)  # Number of folds for cross-validation

cvModel = crossval.fit(crime_data)

evaluator_r2 = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="r2")
evaluator_mae = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="mae")

predictions = cvModel.transform(crime_data)
rmse = evaluator.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)
mae = evaluator_mae.evaluate(predictions)

print("Root Mean Squared Error (RMSE):", rmse)

best_model = cvModel.bestModel.stages[-1]  #  that grabs the best model found by the cross val and param grid

best_model_path = "/user/jdy2003/best_xgb_model"
predictions.show(10)

predictions = predictions.drop(col("features"))
predictions.show(10)

predictions.write.format("csv").option("header", "true").mode("overwrite").save("/user/jdy2003/predictions")

print("Root Mean Squared Error (RMSE):", rmse)
print("R squared value: ", r2)
print("MAE score ", mae)


spark.stop()