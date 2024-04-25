import pyspark # type: ignore

from pyspark.sql import SparkSession 
from pyspark.sql.functions import col 

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("benchmark model").getOrCreate()

crime_data = spark.read.csv("/user/jdy2003/nycFrame/", header=True)

crime_data = crime_data.filter(col("response_time_in_minutes") < 10)

model_path = "/user/jdy2003/xgbModel/"

loaded_model = PipelineModel.load(model_path)

predictions_df = loaded_model.transform(crime_data)

predictions_df.show(10)

spark.stop()
