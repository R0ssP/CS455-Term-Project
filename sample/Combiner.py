from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
os.environ["PYSPARK_PYTHON"] = "/s/bach/j/under/jdy2003/miniconda3/bin/python3.12"

spark = SparkSession.builder.appName("Combine").getOrCreate()
part0 = spark.read.csv("fPartition_0", header=True)
part1 = spark.read.csv("fPartition_1", header=True)
part2 = spark.read.csv("fPartition_2", header=True)
part3 = spark.read.csv("fPartition_3", header=True)

part0.show(10)
part1.show(10)
part2.show(10)
part3.show(10)

p0count = part0.count()

p1count = part1.count()

p2count = part2.count()

p3count = part3.count()


nycCrime = part0.union(part1).union(part2).union(part3)

print("PRINTING COUNT INFORMATION")
print(p0count, " " , p1count, " ", p2count, " ", p3count)

crimeCount = nycCrime.count()
print("PRINTING TOTAL COUNT")
print(crimeCount)
print("WE FUCKING MADE IT")

nycCrime.write.mode("overwrite").option("header","true").csv("/user/jdy2003/nycFrame")

