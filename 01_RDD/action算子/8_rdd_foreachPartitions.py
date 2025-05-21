from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    # 和foreach一样，没有返回值，但一次处理的是一个分区的数据
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)
    rdd.foreachPartition(lambda item: print(list(item)))
