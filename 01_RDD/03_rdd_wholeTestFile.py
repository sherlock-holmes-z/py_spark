from pyspark import SparkConf, SparkContext


if __name__ == '__main__':

    conf = SparkConf().setMaster("local").setAppName("rdd_textFile")
    sc = SparkContext(conf=conf)

    # 读取小文件文件夹
    rdd = sc.wholeTextFiles("../data/input")

    # 获取的是路径文件名和文件内容的元祖组成的列表
    print(rdd.collect())

    # 只获取文件内容
    rdd = rdd.map(lambda x : x[1])
    print(rdd.collect())