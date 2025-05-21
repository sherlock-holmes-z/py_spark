from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5)])

    def method(k):
        if k == 'a' or k == 'b' or k == 'c':
            return 1
        else:
            return 0

    # 对rdd进行自定义分区，必须是kv形式
    #   参数一：重新分区后有几个分区
    #   参数二：自定义分区规则，函数传入key，返回值一定是int类型的分区编号，不能超过参数1减一
    r1 = rdd.partitionBy(2, method)
    print(r1.glom().collect())
