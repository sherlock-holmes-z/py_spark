from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("flatmap")
    sc = SparkContext(conf=conf)

    #   只对二元元祖生效
    rdd = sc.parallelize([("a",1),("b",2),("c",3)])
    # rdd = sc.parallelize({"a": 1, "b": 2, "c": 3})

    rdd2 = rdd.mapValues(lambda x: x * 2)

    print(rdd2.collect())
