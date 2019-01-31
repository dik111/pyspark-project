# -*- coding: UTF-8 -*-
from pyspark import SparkConf, SparkContext


def my_map():
    """
    map(func)
	将func函数作用到数据集的每一个元素上，生成一个新的分布式的数据集返回

	word => (word,1)
    :return:
    """
    data = [1, 2, 3, 4, 5]
    rdd1 = sc.parallelize(data)
    rdd2 = rdd1.map(lambda x: x * 2)
    print(rdd2.collect())


def my_map2():
    a = sc.parallelize(["dog", "tiger", "lion", "cat", "panther", " eagle"])
    b = a.map(lambda x: (x, 1))
    print(b.collect())


def my_filter():
    """
    filter(func)
	选出所有func返回值为true的元素，生成一个新的分布式的数据集返回
    :return:
    """
    data = [1, 2, 3, 4, 5]
    rdd1 = sc.parallelize(data)
    mapRdd = rdd1.map(lambda x: x * 2)
    filterRdd = mapRdd.filter(lambda x: x > 5)
    print(filterRdd.collect())

    print(sc.parallelize(data).map(lambda x: x * 2).filter(lambda x: x > 5).collect())


def my_flatMap():
    """
    flatMap(func)
	输入的item能够被map到0或者多个items输出，返回值是一个Sequence
    :return:
    """
    data = ["hello spark", "hello world", "hello world"]
    rdd = sc.parallelize(data)
    print(rdd.flatMap(lambda line: line.split(" ")).collect())


def my_groupBy():
    """
    groupByKey：把相同的key的数据分发到一起
    :return:
    """
    data = ["hello spark", "hello world", "hello world"]
    rdd = sc.parallelize(data)
    mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
    groupByRdd = mapRdd.groupByKey()
    print(groupByRdd.collect())
    print(groupByRdd.map(lambda x: {x[0]: list(x[1])}).collect())


def my_reduceByKey():
    data = ["hello spark", "hello world", "hello world"]
    rdd = sc.parallelize(data)
    mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
    reduceBykeyRdd = mapRdd.reduceByKey(lambda a, b: a + b)
    print(reduceBykeyRdd.collect())


def my_sort():
    """
    reduceByKey: 把相同的key的数据分发到一起并进行相应的计算
	 mapRdd.reduceByKey(lambda a,b:a+b)
	 [1,1]  1+1
	 [1,1,1]  1+1=2+1=3
	 [1]    1
    :return:
    """
    data = ["hello spark", "hello world", "hello world"]
    rdd = sc.parallelize(data)
    mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
    reduceBykeyRdd = mapRdd.reduceByKey(lambda a, b: a + b)
    reduceByValueRdd = reduceBykeyRdd.map(lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0])).collect()

    print(reduceByValueRdd)


def my_union():
    a = sc.parallelize([1, 2, 3])
    b = sc.parallelize([3, 4, 5])
    unionAB = a.union(b).collect()
    print(unionAB)


if __name__ == '__main__':
    conf = SparkConf().setAppName('local[2]')
    sc = SparkContext(conf=conf)

    my_union()

    sc.stop()
