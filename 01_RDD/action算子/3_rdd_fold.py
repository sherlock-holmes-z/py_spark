from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 10), 3)

    # action算子
    # 先分区内聚合，再分区间聚合，都具有初始值
    # 划分三个分区
    # [1,2,3] [4,5,6]   [7,8,9]
    #   [10+6   10+15    10+24]
    # 10+16+25+34 = 85
    r1 = rdd.fold(10, lambda x, y: x + y)
    print(r1)
