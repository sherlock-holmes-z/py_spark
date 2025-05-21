from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[1]").setAppName("app")
    sc = SparkContext(conf=conf)

    # foreach和saveAsTextFile都是由分区excutor直接执行的，跳过driver
    # 其余action算子都会将结果发送至driver

    # 写入数据到指定目录，连接远程py环境会写入到远程服务器上，也可以写入到hdfs
    rdd = sc.parallelize([('a', 1), ('b', 2), ('c', 3)])
    rdd.saveAsTextFile("../../data/output/1")

    # 有几个分区，就会写入几个文件
    rdd = sc.parallelize([('a', 1), ('b', 2), ('c', 3)], 3)
    rdd.saveAsTextFile("../../data/output/2")
