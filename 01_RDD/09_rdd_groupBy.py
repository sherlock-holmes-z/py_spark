from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6])

    #   生成的结果为key-iterator
    rdd1 = rdd.groupBy(lambda x: "o" if (x % 2 == 0) else "j")

    #   将iterator转成list方便输出
    rdd2 = rdd1.map(lambda x: (x[0], list(x[1])))

    print(rdd2.collect())