from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
os.environ["PYSPARK_PYTHON"] = "/s/bach/j/under/jdy2003/miniconda3/bin/python3.12"

spark = SparkSession.builder.appName("Combine").getOrCreate()
part0 = spark.read.csv("fPartition_0", header=True)
part1 = spark.read.csv("fPartition_1", header=True)
part2 = spark.read.csv("fPartition_2", header=True)
part3 = spark.read.csv("fPartition_3", header=True)

crash_data = spark.read.csv("/user/jdy2003/crashes/", header=True)

part0.show(10)
part1.show(10)
part2.show(10)
part3.show(10)

p0count = part0.count()

p1count = part1.count()

p2count = part2.count()

p3count = part3.count()


nycCrime = part0.union(part1).union(part2).union(part3)

nycCrime = nycCrime.join(crash_data, ["DATE", "zone"], "left")

nycCrime = nycCrime.fillna("0", subset=["accident_count"])

nycCrime = nycCrime.filter(F.col("response_time_in_minutes") < 10)

# Order the distinct event_type values in descending order

mapping_expression = (
    F.when(F.col("TYP_DESC").contains("AMBULANCE"),0)
    .when(F.col("TYP_DESC").contains("VEHICLE ACCIDENT"),1)
    .when(F.col("TYP_DESC").contains("FIRE"),2)
    .otherwise(3) #  this is a police response
)

nycCrime = nycCrime.withColumn("event_type_value", mapping_expression)

# print("PRINTING COUNT INFORMATION")
# print(p0count, " " , p1count, " ", p2count, " ", p3count)
nycCrime.write.mode("overwrite").option("header","true").csv("/user/jdy2003/nycFrameTest")
