from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6])

    # 结果为true时保留
    r1 = rdd.filter(lambda x: x % 2 == 0)

    print(r1.collect())