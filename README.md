                                                                        setting up file in pyspark

1. import sys

2. use "whereis pyspark" in the terminal to find the path to pyspark

3. copy the path util the version id for example 3.5.0-with-hadoop3.3

4. replace the spark_python_path variable with your path to spark

5. ensure you have your hadoop cluster and spark cluster up and running 

    5a. command to start hadoop $HADOOP_HOME/sbin/start-dfs.sh

    5b. command to start spark master $SPARK_HOME/sbin/start-master.sh

    5c. command to start spark workers $SPARK_HOME/sbin/start-workers.sh

6. now try to run the sample .py, you should get an error saying that neworleans.txt is not in hdfs under a certain directory,

create that directory in hadoop using this command: $HADOOP_HOME/bin/hadoop fs -mkdir /path/to/your/directory

7. now put the neworleans.txt file in the directory using this command: 

    $HADOOP_HOME/bin/hadoop fs -put neworleans.txt /path/to/your/hdfs/directory
    


now you can experiment with spark
