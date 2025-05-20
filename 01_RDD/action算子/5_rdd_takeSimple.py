from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('d', 4)])

    # 随机取多少条数据
    # 参数一：是否可以重复取，参数二：取多少条，（参数三：随机算子，默认就是随机算子，一般不传，传同一个值每次都会取同样的结果）
    r1 = rdd.takeSample(True, 10, 1)
    print(r1)

    #   num超过数据量，不能重复就只取所有
    r1 = rdd.takeSample(False, 10)
    print(r1)