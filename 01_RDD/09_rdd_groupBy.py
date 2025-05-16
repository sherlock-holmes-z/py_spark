from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a',1),('b',2),('c',3),('a',2),('b',3),('c',4)])

    #   生成的结果为key-iterator, iterator中为源数据完整的k-v
    rdd1 = rdd.groupBy(lambda x: x[0])

    #   将iterator转成list方便输出
    rdd2 = rdd1.map(lambda x: (x[0], list(x[1])))
    print(rdd2.collect())

    # r1 = sc.parallelize([1, 2, 3, 4, 5, 6, 2, 3, 4])
    # r2 = r1.map(lambda x: (x, x))
    # r3 = r2.groupByKey()
    # for k, v in r3.collect():
    #     print(k, list(v))
