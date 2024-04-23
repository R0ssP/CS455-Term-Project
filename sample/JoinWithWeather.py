from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from Util import scrub_colum_array


spark = SparkSession.builder.appName("joinWithWeather").getOrCreate()

crime_df = spark.read.csv("iPartition_2", header=True)

weather_df = spark.read.csv("NY_weather.csv", header=True)
weather_df = weather_df.select("DATE", "PRCP", "TMIN", "TMAX")

# Calculate the average of TMIN and TMAX and add as a new column 'TAVG'
weather_df = weather_df.withColumn("TAVG", (F.col("TMIN") + F.col("TMAX")) / 2)

# Join DataFrames on DATE column
final_weather_df = weather_df.select("DATE", "PRCP", "TAVG")
final_weather_df = final_weather_df.withColumn("DATE", F.date_format(F.col("DATE"), "MM/dd/yyyy"))

crime_df = crime_df.join(final_weather_df, on='DATE', how='left') 
crime_df.show(10)

crime_df.write.format("csv").option("header", "true").mode("overwrite").save("/user/jdy2003/fPartition_2")