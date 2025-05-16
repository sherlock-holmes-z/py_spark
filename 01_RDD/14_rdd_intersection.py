from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('b', 1), ('d', 4)])
    rdd2 = sc.parallelize([('a', 1), ('b', 2), ('c', 3)])

    # 交集
    r1 = rdd1.intersection(rdd2)
    print(r1.collect())