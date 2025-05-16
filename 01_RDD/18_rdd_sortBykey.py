from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('A', 2), ('B', 100), ('b', 2), ('c', 3)])

    # 根据key排序，可以通过keyFunc只对key进行处理
    r1 = rdd.sortByKey(ascending=True,numPartitions=1,keyfunc=lambda x : str(x).lower())
    print(r1.collect())

    r2 = rdd.sortByKey(ascending=True,numPartitions=1)
    print(r2.collect())