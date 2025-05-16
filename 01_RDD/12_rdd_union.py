from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('b', 2)])
    rdd2 = sc.parallelize([('a', 1), 1])

    # union合并两个rdd,  注意不是并集,后续使用distinct去重后才是并集
    r1 = rdd1.union(rdd2).distinct()

    print(r1.collect())

