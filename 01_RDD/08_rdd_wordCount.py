from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("flatmap")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(["java python scala1", "vue node", "java c++ scala1"])

    rdd1 = rdd.flatMap(lambda x: x.split(" "))

    rdd2 = rdd1.map(lambda x: (x, 1))

    rdd3 = rdd2.reduceByKey(lambda x, y: x + y)

    # 倒序取前两位，大数据场景下无需全局排序
    print(rdd3.top(2, key=lambda x: x[1]))
    # 正序取前两位，大数据场景下无需全局排序
    print(rdd3.takeOrdered(2, key=lambda x: x[1]))
