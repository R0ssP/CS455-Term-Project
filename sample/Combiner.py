from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from Util import scrub_colum_array

spark = SparkSession.builder.appName("Combine").getOrCreate()
part0 = spark.read.csv("fPartition_0", header=True)
part1 = spark.read.csv("fPartition_1", header=True)
part2 = spark.read.csv("fPartition_2", header=True)
# part3 = spark.read.csv("fPartition_3", header=True)

p0count = part0.count()

p1count = part1.count()

p2count = part2.count()

# nycCrime = part0.union(part1).union(part2).union(part3)

print("PRINTING COUNT INFORMATION")
print(p0count, " " , p1count, " ", p2count)

#crimeCount = nycCrime.count()
#print("PRINTING TOTAL COUNT")
#print(crimeCount)

