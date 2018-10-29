#!/usr/bin/env python3      
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# pyspark 的方式启动，这里我本地的spark使用的是spark 2.3.1 版本。如果是其他spark版本，mongo-spark-connector的版本号是不一样的，具体查看mongodb的官方文档
# pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.1


# spark-submit的方式提交，我才用的是nohup的方式提交
# nohup spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.1 ./read_mongo_schema.py >> ./read_mongo_schema.log &


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('MyApp') \
        .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1/test.user') \
        .getOrCreate()

    # 如果mongodb中的json字段太多，我们也可以通过schema限制，过滤掉不要的数据
    # name 设置为StringType
    # age 设置为IntegerType
    schema = StructType([
        StructField("name", StringType()),
        StructField("age", IntegerType())
    ])

    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').schema(schema).load()

    df.createOrReplaceTempView('user')

    resDf = spark.sql('select * from user')

    resDf.show()

    spark.stop()

    exit(0)
