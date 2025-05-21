from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 1), ('b', 4),('a',2), ('c',3)])

    # 根据key分组，没有聚合功能
    # 只能用于kv类型, 生成k-iterator结果，iterator中有只有源数据中的value
    r1 = rdd.groupByKey()
    for k,v in r1.collect():
        print(k, list(v))