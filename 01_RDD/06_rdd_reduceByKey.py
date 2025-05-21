from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("flatmap")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(["java python scala","vue node","java c++ scala"])

    rdd2 = rdd.flatMap(lambda x : x.split(" "))

    rdd3 = rdd2.map(lambda x : (x,1))

    # 根据key分组，再对value进行聚合
    rdd4 = rdd3.reduceByKey(lambda x,y : x + y)

    print(rdd4.sortBy(lambda x : x[1],ascending=False).collect())
