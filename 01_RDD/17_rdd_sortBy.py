from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 1), ('b', 100), ('a', 2), ('c', 3)], 3)

    # 如果要全局有序，排序分区numPartitions要设置为1，否则只能保证分区内局部有序
    r1 = rdd.sortBy(lambda x: ord(x[0])+ x[1], ascending=True, numPartitions=3)
    print(r1.collect())

    # ord() 获取字符串的码值
    for k, v in r1.collect():
        print(ord(k)+ v)
