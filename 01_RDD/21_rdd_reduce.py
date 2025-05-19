from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    r1 = sc.parallelize((1, 2, 3, 4, 5))
    # 将rdd数据集按照传入的逻辑进行聚合，返回一个结果，action算子
    print(r1.reduce(lambda x, y: x + y))

    r2 = sc.parallelize([('a', 1), ('a', 2), ('B', 100), ('b', 2), ('c', 3)])
    print(r2.reduce(lambda x, y: x + y))

