from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss = SparkSession.builder.appName("ss").master("local").getOrCreate()

    ss_rdd = ss.sparkContext.parallelize([1, 2, 3, 4, 5])

    sum = ss_rdd.reduce(lambda x, y: x * y)

    print(sum)
