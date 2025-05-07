from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("flatmap")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(["java python scala","vue node"])

    rdd2 = rdd.map(lambda x : x.split(" "))
    print(rdd2.collect())

    #   解除嵌套
    rdd3 = rdd.flatMap(lambda x : x.split(" "))
    print(rdd3.collect())

