from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import matplotlib.pyplot as plt
import tempfile
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

import os
os.environ["PYSPARK_PYTHON"] = "/s/bach/j/under/jdy2003/miniconda3/bin/python3.12"

spark = SparkSession.builder.appName("analysis").getOrCreate()

# Load the model with the specified schema

path_to_model = '/user/jdy2003/best_xgb_model'
predictions = spark.read.csv("/user/jdy2003/predictions", header=True)
predictions.show()

zone_predictions = predictions.groupBy("zone") \
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
print("WORST PERFORMING ZONES")
worst_zones.show() 

# BELOW IS THE VISUALIZATION

# predictions_pandas = predictions.select("prediction", "response_time_in_minutes").toPandas() # gets the fields we want
# plt.figure(figsize=(8, 6)) # 10 inches by 6 inches yo
# plt.scatter(predictions_pandas["prediction"], predictions_pandas["response_time_in_minutes"], alpha=0.5) # alpha = .5 is semitransparent markers
# plt.xlabel("Predicted Response Time (minutes)")
# plt.ylabel("Actual Response Time (minutes)")
# plt.title("Scatter Plot: Predicted vs Actual Response Time")

# temp_dir = tempfile.mkdtemp()
# plot_path = os.path.join(temp_dir, "scatter_plot.png")
# plt.savefig(plot_path, format='png')

# # Print the path to the saved plot
# print(f"Scatter plot saved at: {plot_path}")

#  that shit above takes FOREVER to run lol

# BELOW IS SEGMENTATION ANALYSIS
segmented_analysis = predictions.groupBy("TYP_DESC").agg(F.avg("prediction").alias("avg_prediction"))
segmented_analysis.show()


# BELOW IS FEATURE IMPORTANCE / CORRELATION

cast_columns = ['TAVG', 'PRCP', 'event_type_value', 'zone', 'DayOfYear',
                'response_time_in_minutes', 'Latitude', 'Longitude', 'accident_count']
for column in cast_columns:
    predictions = predictions.withColumn(column, F.col(column).cast("float"))

assembler = VectorAssembler(inputCols=["zone", "TAVG", "PRCP", "accident_count", "event_type_value"],
                            outputCol="features")

predictions = assembler.transform(predictions)

lr = LinearRegression(featuresCol="features", labelCol="response_time_in_minutes")

predictions = predictions.drop(F.col("prediction"))
model = lr.fit(predictions)

print("Feature Coefficients:")
coefficients = model.coefficients
for i, col in enumerate(assembler.getInputCols()):
    print(f"{col}: {coefficients[i]}")


spark.stop()