
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
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.appName("train model").getOrCreate()

crime_data = spark.read.csv("/user/jdy2003/nycFrame/", header=True)

# column names that we gotta cast
cast_columns = ['TAVG', 'PRCP', 'event_type_value', 'zone', 'DayOfYear',
                'response_time_in_minutes', 'Latitude', 'Longitude', 'accident_count']
for column in cast_columns:
    crime_data = crime_data.withColumn(column, col(column).cast("float"))

# double check no null values exist

crime_data.na.drop()

feature_cols = ['event_type_value', 'zone','DayOfYear', 'TAVG', 'PRCP', 'accident_count']

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data_features = assembler.transform(crime_data)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
scaled_data = scaler.fit(data_features).transform(data_features)

# k means model defined below
kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster_prediction", k=6, seed=123)

# below fits the model
kmeans_model = kmeans.fit(scaled_data)

# gets predictions of the clusters
clustered_data = kmeans_model.transform(scaled_data)

# incorporate cluster predictions into feature set!
cluster_features = ['cluster_prediction']
final_feature_cols = feature_cols + cluster_features
assembler = VectorAssembler(inputCols=final_feature_cols, outputCol="final_features")
final_data = assembler.transform(clustered_data)

gbt = GBTRegressor(featuresCol="final_features", labelCol="response_time_in_minutes")

# Define parameter grid for hyperparameter tuning
paramGrid = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [5, 10]) \
    .addGrid(gbt.maxBins, [20, 40]) \
    .addGrid(gbt.minInstancesPerNode, [1, 10]) \
    .addGrid(gbt.minInfoGain, [0.01, 0.001]) \
    .addGrid(gbt.subsamplingRate, [0.5, 0.75]) \
    .build()

# Define evaluator
evaluator_rmse = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="rmse")
evaluator_mae = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="mae")
evaluator_r2 = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="r2")

# Define cross-validator
crossval = CrossValidator(estimator=gbt,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator_rmse,
                          numFolds=7)  # Number of folds for cross-validation

# use cross validation to fit
cvModel = crossval.fit(final_data)

# evaluate
predictions = cvModel.transform(final_data)
rmse = evaluator_rmse.evaluate(predictions)
mae = evaluator_mae.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

best_model = cvModel.bestModel
best_model_path = "/user/jdy2003/best_gbt_model"
predictions_path = "/user/jdy2003/predictions"

predictions.show(20)

output_df = predictions.select(
    "event_type_value", 
    "zone", 
    "DayOfYear", 
    "TAVG", 
    "PRCP", 
    "accident_count", 
    "cluster_prediction", 
    "prediction", 
    "response_time_in_minutes"
)

best_model.write().overwrite().save(best_model_path)
output_df.write.mode("overwrite").option("header", "true").csv(predictions_path)

# show the metrics
print("Root Mean Squared Error (RMSE):", rmse)
print("Mean Absolute Error (MAE):", mae)
print("R-squared (R^2) Value:", r2)
# Stop Spark session
spark.stop()