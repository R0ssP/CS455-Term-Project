from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
import matplotlib.pyplot as plt

import os
os.environ["PYSPARK_PYTHON"] = "/s/bach/j/under/jdy2003/miniconda3/bin/python3.12"

spark = SparkSession.builder.appName("analysis").getOrCreate()

# load model and the data
path_to_model = '/user/jdy2003/best_xgb_model'
crime_data = spark.read.csv("/user/jdy2003/nycFrame", header=True)

crime_predictions = spark.read.csv("/user/jdy2003/predictions", header=True)

zone_predictions = crime_predictions.groupBy("zone") \
    .agg(
        F.avg("prediction").alias("avg_prediction"),
        F.avg("response_time_in_minutes").alias("avg_response_time"),
        F.avg("Latitude").alias("avg_latitude"),
        F.avg("Longitude").alias("avg_longitude")
    )

best_zones = zone_predictions.orderBy(F.col("avg_prediction").asc())
worst_zones = zone_predictions.orderBy(F.col("avg_prediction").desc())

print("BEST PERFORMING ZONES")
best_zones.show()   
best_zones.write.mode("overwrite").option("header", "true").csv("/user/jdy2003/bestZones")
print("WORST PERFORMING ZONES")
worst_zones.show() 
worst_zones.write.mode("overwrite").option("header", "true").csv("/user/jdy2003/bestZones")



# BELOW IS THE AREA FOR METRICS MAE, R^2 RMSE
evaluator_rmse = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="rmse")
evaluator_mae = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="mae")
evaluator_r2 = RegressionEvaluator(labelCol="response_time_in_minutes", predictionCol="prediction", metricName="r2")

crime_predictions = crime_predictions.withColumn("prediction", F.col("prediction").cast("float"))
crime_predictions = crime_predictions.withColumn("response_time_in_minutes", F.col("response_time_in_minutes").cast("float"))


rmse = evaluator_rmse.evaluate(crime_predictions)
mae = evaluator_mae.evaluate(crime_predictions)
r2 = evaluator_r2.evaluate(crime_predictions)

metric_path = "/s/bach/j/under/jdy2003/metrics.txt"

with open(metric_path, 'w') as file:
    file.write(f"{"Root Mean Squared Error (RMSE):"} {rmse} \n")
    file.write(f"{"Mean Average Error (MAE): "} {mae} \n")
    file.write(f"{"R Squared Value: "} {r2} \n")

print("Metrics written to file at ", metric_path)
# # BELOW IS FEATURE IMPORTANCE / CORRELATION
cast_columns = ['TAVG', 'PRCP', 'event_type_value', 'zone', 'DayOfYear',
                'response_time_in_minutes', 'Latitude', 'Longitude', 'accident_count']
for column in cast_columns:
    crime_predictions = crime_predictions.withColumn(column, F.col(column).cast("float"))

assembler = VectorAssembler(inputCols=["zone", "TAVG", "PRCP", "accident_count", "event_type_value"],
                            outputCol="features")

crime_predictions = assembler.transform(crime_predictions)

lr = LinearRegression(featuresCol="features", labelCol="response_time_in_minutes")

coefficients = crime_predictions.drop(F.col("prediction"))
model = lr.fit(coefficients)

print("Feature Coefficients:")
coefficients = model.coefficients

file_path = "/s/bach/j/under/jdy2003/correlation.txt"

with open(file_path, 'w') as file:
    file.write("Feature Coefficients:\n")
    
    for i, col in enumerate(assembler.getInputCols()):
        file.write(f"{col}: {coefficients[i]}\n")

print("Feature coefficients have been written to ", file_path)



# BELOW IS RESIDUAL ANALYSIS
crime_predictions = crime_predictions.withColumn("residuals", col("response_time_in_minutes") - col("prediction"))
residual_panda = crime_predictions.select("residuals").toPandas()
plt.figure(figsize=(8, 6))
plt.scatter(range(len(residual_panda)), residual_panda["residuals"], alpha=0.5)
plt.xlabel("Index")
plt.ylabel("Residuals")
residual_dir = "/s/bach/j/under/jdy2003/images/residual.png"
plt.savefig(residual_dir,format="png")


#BELOW IS TIME SERIES ANALYSIS
time_series = crime_predictions.groupBy("DayOfYear").agg(F.avg("response_time_in_minutes").alias('avg_response_time'))
time_series_panda = time_series.toPandas()
plt.figure(figsize=(8,6))
plt.plot(time_series_panda["DayOfYear"],time_series_panda["avg_response_time"])
plt.xlabel("Day of Year")
plt.ylabel("Average Response Time (minutes)")
plt.title("Average Response Time Over Time")
time_series_dir = "/s/bach/j/under/jdy2003/images/time_series.png"
plt.savefig(time_series_dir, format="png")

# BELOW IS THE PREDICTION PLOT
predictions_pandas = crime_predictions.select("prediction", "response_time_in_minutes").toPandas() # gets the fields we want
plt.figure(figsize=(8, 6)) # 8 inches by 6 inches yo
plt.scatter(predictions_pandas["prediction"], predictions_pandas["response_time_in_minutes"], alpha=0.5) # alpha = .5 is semitransparent markers
plt.xlabel("Predicted Response Time (minutes)")
plt.ylabel("Actual Response Time (minutes)")
plt.title("Scatter Plot: Predicted vs Actual Response Time")

predictions_path = "/s/bach/j/under/jdy2003/images/scatter_plot.png"
plot_path = os.path.join(predictions_path, "scatter_plot.png")
plt.savefig(plot_path, format='png')

spark.stop()