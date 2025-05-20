from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([3, 5, 1, 7, 3, 9, 1, 3])

    # 正序排序，取前三
    print(rdd.takeOrdered(3))
    # 降序排序，取前三
    print(rdd.takeOrdered(3, lambda x: -x))
