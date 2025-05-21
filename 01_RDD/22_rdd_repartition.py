from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5)])

    # 对rdd进行重新分区，不建议使用，除非要全局排序，将分区设置为1,(增加分区涉及shuffle,性能较差)
    r1 = rdd.repartition(3)
    print(r1.glom().collect())

    # 对rdd进行重新分区，shuffle默认false，如果增加分区必须指定shuffle为true
    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(2, shuffle=True).getNumPartitions())
