from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 2), ('B', 100), ('b', 2), ('c', 3)])
    # action算子
    # 将rdd各个分区的数据，统一收集到一个driver中，形成一个list对象
    # 需要注意rdd是分布式对象，数据量可能很大，所以使用前要明确数据量不是很大，否则会把内存撑爆
    print(rdd.collect())