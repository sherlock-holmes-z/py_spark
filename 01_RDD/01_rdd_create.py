from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]")
sc = SparkContext(conf=conf)

# parallelize： 本地列表/集合 -》 分布式对象
# 不指定分区数，连接的py环境是几核，就会有几个分区
rdd = sc.parallelize([1, 2, 3, 4, 5,6])
print(rdd.getNumPartitions())

# 指定分区数
rdd  = sc.parallelize({1, 2, 3, 4, 5},3)
print(rdd.getNumPartitions())

# collect是将rdd（分布式对象）中每个分区的数据，都发送到driver中，形成一个py 列表对象
print("rdd的内容：",rdd.collect())