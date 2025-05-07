from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("map")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5])

    # 定义方法作为算子的传入载体
    def add_1(num):
        return num + 1

    print(rdd.map(add_1).collect())

    #   直接使用匿名函数，适用于一行能解决的函数体
    print(rdd.map(lambda x : x + 1).collect())