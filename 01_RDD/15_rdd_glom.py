from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    # 设置好分区
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7], 3)

    # 将rdd的数据按照分区来输出
    r1 = rdd.glom()
    print(r1.collect())

    # 解除嵌套后恢复不分区的状态
    r2 = r1.flatMap(lambda x: x)
    print(r2.collect())
