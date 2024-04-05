import sys
spark_python_path = '/usr/local/spark/3.5.0-with-hadoop3.3/python'
sys.path.append(spark_python_path)
import pyspark
from pyspark.sql import Row
from pyspark.sql import SparkSession


spark = SparkSession.builder \
.master("local")\
.appName('word_count')\
.getOrCreate()

neworleans_frame = spark.read.text("neworleans.txt")


count = neworleans_frame.count()

print(count)
