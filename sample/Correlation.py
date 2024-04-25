from pyspark.sql import SparkSession
from pyspark.sql.functions import col, corr

spark = SparkSession.builder.appName("correlation").getOrCreate()

crime_data = spark.read.csv("/user/jdy2003/nycFrame/", header=True)

crime_data = crime_data.select(
    col("response_time_in_minutes").cast("float"),
    col("TAVG").cast("float"),
    col("PRCP").cast("float"),
    col("event_type_value").cast("float"),
    col("zone").cast("float"),
    col("DayOfYear").cast("float")
)


crime_data = crime_data.na.drop()

corr_TAVG = crime_data.stat.corr("response_time_in_minutes", "TAVG")
corr_PRCP = crime_data.stat.corr("response_time_in_minutes", "PRCP")
corr_event_type = crime_data.stat.corr("response_time_in_minutes", "event_type_value")
corr_zone = crime_data.stat.corr("response_time_in_minutes", "zone")
corr_DayOfYear = crime_data.stat.corr("response_time_in_minutes", "DayOfYear")

print("Correlation between Response Time and TAVG:", corr_TAVG)
print("Correlation between Response Time and PRCP:", corr_PRCP)
print("Correlation between Response Time and Event Type Value:", corr_event_type)
print("Correlation between Response Time and Zone:", corr_zone)
print("Correlation between Response Time and Day of Year:", corr_DayOfYear)

spark.stop()