# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     0801
   Description :
   Author :       dik
   date：          2019/2/6
-------------------------------------------------
   Change Activity:
                   2019/2/6:
-------------------------------------------------
"""


__author__ = 'dik'

from pyspark.sql import SparkSession,Row

def basic(spark):
    df = spark.read.json("/opt/module/spark-2.3.2-bin-hadoop2.7/examples/src/main/resources/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()


def schema_inference_example():
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("/opt/module/spark-2.3.2-bin-hadoop2.7/examples/src/main/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

    # Infer the schema, and register the DataFrame as a table.
    schemaPeople = spark.createDataFrame(people)
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are Dataframe objects.
    # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teenNames:
        print(name)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark0801").getOrCreate()
    schema_inference_example()
    spark.stop()