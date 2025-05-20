from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2)])
    # first:获取rdd中第一个元素
    r1 = rdd.first()
    print(r1)
    rdd = sc.parallelize([1, 2, 3, 4, 5])
    r2 = rdd.first()
    print(r2)

    # take 获取前n个元素，不管获取几个，返回结果都是list
    rdd = sc.parallelize([1, 2, 3, 4, 5])
    r3 = rdd.take(3)
    print(r3)

    rdd = sc.parallelize([6, 2, 3, 5, 5])
    # 获取排名前几的元素
    print('top:', rdd.top(3))
    rdd = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
    # 指定排序字段，获取排名前几
    print('top key:', rdd.top(2, key=lambda x: x[1]))

    rdd = sc.parallelize([6, 2, 3, 5, 5])
    print('count:', rdd.count())
