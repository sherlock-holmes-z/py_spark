from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a',1),('b',2),('c',3),('a',1),('b',2),('c',4)])

    # 去重
    r1 = rdd.distinct()

    print(r1.collect())
