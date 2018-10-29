#!/usr/bin/env python3      
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession


# pyspark 的方式启动，这里我本地的spark使用的是spark 2.3.1 版本。如果是其他spark版本，mongo-spark-connector的版本号是不一样的，具体查看mongodb的官方文档
# pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.1


# spark-submit的方式提交，我才用的是nohup的方式提交
# nohup spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.1 ./read_mongo.py >> ./read_mongo.log &


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('MyApp') \
        .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1/test.user') \
        .getOrCreate()

    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()

    df.createOrReplaceTempView('user')

    resDf = spark.sql('select name,age,sex from user')

    resDf.show()

    spark.stop()

    exit(0)

