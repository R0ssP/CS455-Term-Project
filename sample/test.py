import sys
spark_python_path = '/usr/local/spark/3.5.0-with-hadoop3.3/python'
sys.path.append(spark_python_path)
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Add Zone Column").getOrCreate()

# Sample DataFrame
data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
fake_list = [1,2,3,4]
df = spark.createDataFrame(data, ["col1", "col2", "col3"])
df.show()

# Define a function to be applied to each row
def add_zone(row, fake_list):
    # Example function: add the values of col1 and col2
    zone_value = row[0] + row[1] + fake_list[0]
    return row + (zone_value,)

# Convert DataFrame to RDD
rdd = df.rdd

# Apply the function to each row of the RDD
new_rdd = rdd.map(lambda row: add_zone(row, fake_list))

# Convert RDD back to DataFrame with the new 'zone' column
new_df = new_rdd.toDF(["col1", "col2", "col3", "zone"])

# Show the resulting DataFrame
new_df.show()