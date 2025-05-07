from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("rdd_textFile")
sc = SparkContext(conf=conf)

# 支持本地文件和HDFS
# 参数2可选，表示最小分区数量，但spark有自己的判断，允许范围内有效，超出spark范围就失效
rdd = sc.textFile("../data/input/words.txt",100)

# 开不到100个分区
print(rdd.getNumPartitions())
print(rdd.collect())

rdd2 = sc.textFile("../data/input/words.txt")
rdd2 = rdd2.flatMap(lambda x : x.split(" "))
print(rdd2.collect())

