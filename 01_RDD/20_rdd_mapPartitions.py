from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    # 根据分区对rdd进行批处理，减少资源开销，但要注意内存问题
    r1 = rdd.mapPartitions(lambda item: (x * 2 for x in item))
    print(r1.collect())


    # 可以对一个分区的数据进行批量处理，比如要保存数据到数据库，只需要建立一次连接，之后将所有数据一次保存就行
    # 而map只能一条一条的保存
    def save(item):
        r = list()
        r.append("x")
        for x in item:
            r.append(x * 2)
        return r

    r2 = rdd.mapPartitions(save)
    print(r2.collect())
