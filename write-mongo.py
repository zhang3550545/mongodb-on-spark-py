#!/usr/bin/env python3      
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# pyspark 的方式启动，这里我本地的spark使用的是spark 2.3.1 版本。如果是其他spark版本，mongo-spark-connector的版本号是不一样的，具体查看mongodb的官方文档
# pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.1

# spark-submit的方式提交，我才用的是nohup的方式提交
# nohup spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.1 ./write_mongo.py >> ./write-mongo.log &


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('MyApp') \
        .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1/test.user') \
        .getOrCreate()

    schema = StructType([
        StructField("name", StringType()),
        StructField("age", IntegerType()),
        StructField("sex", StringType())
    ])

    df = spark.createDataFrame([('caocao', 36, 'male'), ('sunquan', 26, 'male'), ('zhugeliang', 26, 'male')], schema)

    df.show()

    df.write.format('com.mongodb.spark.sql.DefaultSource').mode("append").save()

    spark.stop()

    exit(0)
