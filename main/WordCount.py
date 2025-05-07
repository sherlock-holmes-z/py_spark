import os

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    os.environ["PYSPARK_PYTHON"] = "/opt/module/anaconda3/envs/py3.8/bin/python3"
    # os.environ['YARN_CONF_DIR'] = "/opt/module/hadoop-3.1.3/etc/hadoop"
    conf = SparkConf().setAppName("SparkContext_demo").setMaster("yarn")
    sc = SparkContext(conf=conf)
    file_rdd = sc.textFile("hdfs://hadoop202:8020/wcinput/word.txt")
    # file_rdd = sc.textFile("../data/input/words.txt")

    r = file_rdd.flatMap(lambda line: line.split(" "))
    # print(r.collect())
    r = r.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    print(r.collect())