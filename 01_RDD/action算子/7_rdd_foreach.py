from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a',1), ('b',2), ('c',3)])

    # 对rdd的每一个元素执行方法逻辑，但不会有返回值（直接在各自的excuter中执行，不会汇聚到driver，效率比较高）
    r = rdd.foreach(lambda x: print(x))
    # 返回r为none
    print(r)