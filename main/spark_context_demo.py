from typing import Tuple

from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName("SparkContext_demo").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd_data = sc.parallelize([1, 2, 3, 4, 5, 5])

    # 先用map将参数转换为（num，1）的键值对； reduceByKey按key累加
    word_count = rdd_data.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    print(word_count.collect())

    sum = rdd_data.reduce(lambda x, y: x * y)
    print(sum)

    newList = rdd_data.map(lambda x: x + 3)
    print(newList.collect())

    file_rdd = sc.textFile("hdfs://hadoop141:8020/hive/stu.txt")
    print(file_rdd.count(), file_rdd)

    list = file_rdd.flatMap(lambda line: tuple(line.split("\t")))
    print(list.collect())

    list1 = file_rdd.map(lambda line: tuple(line.split("\t")))
    print(list1.collect())
