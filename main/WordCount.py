from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("SparkContext_demo").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    file_rdd = sc.textFile("../data/input/words.txt")

    r = file_rdd.flatMap(lambda line: line.split(" "))
    # print(r.collect())
    r = r.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    print(r.collect())
