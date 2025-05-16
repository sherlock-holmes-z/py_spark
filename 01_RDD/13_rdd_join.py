from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("app")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('b', 1), ('d', 4)])
    rdd2 = sc.parallelize([('a', 2), ('b', 2), ('c', 3)])

    # join算子只能用于二元元组,按key进行关联

    # 结果保留相同key的元组
    r1 = rdd1.join(rdd2)
    print('join:', r1.collect())

    # 保留左边所有，关联右边
    r2 = rdd1.leftOuterJoin(rdd2)
    print('left_join:', r2.collect())
