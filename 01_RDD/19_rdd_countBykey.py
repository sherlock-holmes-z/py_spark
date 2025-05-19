from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 2), ('B', 100), ('b', 2), ('c', 3)])

    # 针对KV类型的数据，通过key出现的次数
    result = rdd.countByKey()
    print(result)